/*
 * Copyright 2019 Confluent Inc.
 */

package io.confluent.connect.jdbc.dialect;

import io.confluent.connect.jdbc.dialect.DatabaseDialectProvider.SubprotocolBasedProvider;
import io.confluent.connect.jdbc.util.ColumnDefinition;
import io.confluent.connect.jdbc.util.ColumnDefinition.Mutability;
import io.confluent.connect.jdbc.util.ColumnDefinition.Nullability;
import io.confluent.connect.jdbc.util.ColumnId;
import io.confluent.connect.jdbc.util.IdentifierRules;
import io.confluent.connect.jdbc.util.TableId;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.kafka.common.config.AbstractConfig;

/**
 * A {@link DatabaseDialect} for Vertica.
 */
public class HiveDatabaseDialect extends GenericDatabaseDialect {

  /**
   *
   */
  public HiveDatabaseDialect(AbstractConfig config) {
    super(config, new IdentifierRules(".", "\"", "\""));
  }


  /**
   * The provider for {@link HiveDatabaseDialect}.
   */
  public static class Provider extends SubprotocolBasedProvider {

    public Provider() {
      super(HiveDatabaseDialect.class.getSimpleName(), "hive2");
    }

    @Override
    public DatabaseDialect create(AbstractConfig config) {
      return new HiveDatabaseDialect(config);
    }

  }

  @Override
  public Map<ColumnId, ColumnDefinition> describeColumns(ResultSetMetaData rsMetadata,
                                                         TableId tableId)
          throws SQLException {
    String schemaName = tableId.schemaName();
    String tableName = tableId.tableName();

    Map<ColumnId, ColumnDefinition> result = new LinkedHashMap<>();
    for (int i = 1; i <= rsMetadata.getColumnCount(); ++i) {
      ColumnDefinition defn = describeColumn(rsMetadata, i, tableId);
      result.put(defn.id(), defn);
    }
    return result;
  }

  protected ColumnDefinition describeColumn(ResultSetMetaData rsMetadata, int column,
                                            TableId tableId)
          throws SQLException {
//    String catalog = schemaName;
//    String schema = null;
    String tableName = tableId.tableName();
//    TableId tableId = new TableId(catalog, schema, tableName);
    String cn = rsMetadata.getColumnName(column);

    String name = cn.substring(tableName.length() + 1);
    String cl = rsMetadata.getColumnLabel(column);
    String alias = cl.substring(tableName.length() + 1);

    ColumnId id = new ColumnId(tableId, name, alias);
    Nullability nullability;
    switch (rsMetadata.isNullable(column)) {
      case ResultSetMetaData.columnNullable:
        nullability = Nullability.NULL;
        break;
      case ResultSetMetaData.columnNoNulls:
        nullability = Nullability.NOT_NULL;
        break;
      case ResultSetMetaData.columnNullableUnknown:
      default:
        nullability = Nullability.UNKNOWN;
        break;
    }
    Mutability mutability = Mutability.MAYBE_WRITABLE;
    if (rsMetadata.isReadOnly(column)) {
      mutability = Mutability.READ_ONLY;
    } else if (false) {
      mutability = Mutability.MAYBE_WRITABLE;
    } else if (rsMetadata.isDefinitelyWritable(column)) {
      mutability = Mutability.WRITABLE;
    }
//    public boolean isSearchable(int column) throws SQLException {
//      throw new SQLFeatureNotSupportedException("Method not supported");
//    }
//
//    public boolean isSigned(int column) throws SQLException {
//      throw new SQLFeatureNotSupportedException("Method not supported");
//    }
//
//    public boolean isWritable(int column) throws SQLException {
//      throw new SQLFeatureNotSupportedException("Method not supported");
//    }
//
//    public boolean isWrapperFor(Class<?> iface) throws SQLException {
//      throw new SQLFeatureNotSupportedException("Method not supported");
//    }
//
//    public <T> T unwrap(Class<T> iface) throws SQLException {
//      throw new SQLFeatureNotSupportedException("Method not supported");
//    }
    return new ColumnDefinition(id, rsMetadata.getColumnType(column),
            rsMetadata.getColumnTypeName(column),
            rsMetadata.getColumnClassName(column), nullability, mutability,
            rsMetadata.getPrecision(column),
            rsMetadata.getScale(column), false,
            rsMetadata.getColumnDisplaySize(column),
            rsMetadata.isAutoIncrement(column), rsMetadata.isCaseSensitive(column),
            false,
            rsMetadata.isCurrency(column), false);
  }

}
