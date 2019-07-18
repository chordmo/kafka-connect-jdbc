/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and limitations under the
 * License.
 */

package io.confluent.connect.jdbc.source;

import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import io.confluent.connect.jdbc.dialect.DatabaseDialect;
import io.confluent.connect.jdbc.util.TableId;

/**
 * TableQuerier executes queries against a specific table. Implementations handle different types of
 * queries: periodic bulk loading, incremental loads using auto incrementing IDs, incremental loads
 * using timestamps, etc.
 */
abstract class TableQuerier implements Comparable<TableQuerier> {
  public enum QueryMode {
    TABLE, // Copying whole tables, with queries constructed automatically
    QUERY // User-specified query
  }

  private final Logger log = LoggerFactory.getLogger(getClass()); // use concrete subclass

  protected final DatabaseDialect dialect;
  protected final QueryMode mode;
  protected final String query;
  protected final String topicPrefix;
  protected final TableId tableId;

  protected long executeTime;

  // Mutable state

  protected long lastUpdate;
  protected PreparedStatement stmt;
  protected ResultSet resultSet;
  protected SchemaMapping schemaMapping;
  private String loggedQueryString;

  public TableQuerier(DatabaseDialect dialect, QueryMode mode, String nameOrQuery,
                      String topicPrefix, long executeTime) {
    this.dialect = dialect;
    this.mode = mode;
    this.tableId = mode.equals(QueryMode.TABLE) ? dialect.parseTableIdentifier(nameOrQuery) : null;
    this.query = mode.equals(QueryMode.QUERY) ? nameOrQuery : null;
    this.topicPrefix = topicPrefix;
    this.lastUpdate = 0;
    this.executeTime = executeTime;
  }

  public long getLastUpdate() {
    return lastUpdate;
  }

  public PreparedStatement getOrCreatePreparedStatement(Connection db) throws SQLException {
    if (stmt != null) {
      return stmt;
    }
    createPreparedStatement(db);
    return stmt;
  }

  protected abstract void createPreparedStatement(Connection db) throws SQLException;

  public boolean querying() {
    return resultSet != null;
  }

  public void maybeStartQuery(Connection db) throws SQLException {
    if (resultSet == null) {
      stmt = getOrCreatePreparedStatement(db);
      resultSet = executeQuery();
      if (resultSet != null) {
        String schemaName = tableId != null ? tableId.tableName() : null; // backwards compatible
        schemaMapping = SchemaMapping.create(schemaName, resultSet.getMetaData(), dialect);
      }
    }
  }

  protected abstract ResultSet executeQuery() throws SQLException;

  public boolean next() throws SQLException {
    return resultSet.next();
  }

  public abstract SourceRecord extractRecord() throws SQLException;

  public void reset(long now) {
    closeResultSetQuietly();
    closeStatementQuietly();
    // TODO: Can we cache this and quickly check that it's identical for the next query
    // instead of constructing from scratch since it's almost always the same
    schemaMapping = null;
    lastUpdate = now;
  }

  private void closeStatementQuietly() {
    if (stmt != null) {
      try {
        stmt.close();
      } catch (SQLException ignored) {
        // intentionally ignored
      }
    }
    stmt = null;
  }

  private void closeResultSetQuietly() {
    if (resultSet != null) {
      try {
        resultSet.close();
      } catch (SQLException ignored) {
        // intentionally ignored
      }
    }
    resultSet = null;
  }

  protected void recordQuery(String query) {
    if (query != null && !query.equals(loggedQueryString)) {
      // For usability, log the statement at INFO level only when it changes
      log.info("Begin using SQL query: {}", query);
      loggedQueryString = query;
    }
  }

  @Override
  public int compareTo(TableQuerier other) {
    if (this.lastUpdate < other.lastUpdate) {
      return -1;
    } else if (this.lastUpdate > other.lastUpdate) {
      return 1;
    } else {
      return this.tableId.compareTo(other.tableId);
    }
  }
}
