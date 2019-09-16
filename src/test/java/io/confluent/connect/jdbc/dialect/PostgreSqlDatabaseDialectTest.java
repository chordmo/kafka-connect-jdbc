/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.connect.jdbc.dialect;

import io.confluent.connect.jdbc.source.JdbcSourceConnectorConfig;
import io.confluent.connect.jdbc.source.JdbcSourceTaskConfig;
import io.confluent.connect.jdbc.util.CachedConnectionProvider;
import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Schema.Type;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;
import org.junit.Test;

import java.io.File;
import java.io.FileWriter;
import java.io.UnsupportedEncodingException;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import io.confluent.connect.jdbc.util.QuoteMethod;
import io.confluent.connect.jdbc.util.TableId;

import static org.junit.Assert.assertEquals;

public class PostgreSqlDatabaseDialectTest extends BaseDialectTest<PostgreSqlDatabaseDialect> {

    @Override
    protected PostgreSqlDatabaseDialect createDialect() {
        return new PostgreSqlDatabaseDialect(sourceConfigWithUrl("jdbc:postgresql://node1:5432"));
    }

    @Test
    public void shouldMapPrimitiveSchemaTypeToSqlTypes() {
        assertPrimitiveMapping(Type.INT8, "SMALLINT");
        assertPrimitiveMapping(Type.INT16, "SMALLINT");
        assertPrimitiveMapping(Type.INT32, "INT");
        assertPrimitiveMapping(Type.INT64, "BIGINT");
        assertPrimitiveMapping(Type.FLOAT32, "REAL");
        assertPrimitiveMapping(Type.FLOAT64, "DOUBLE PRECISION");
        assertPrimitiveMapping(Type.BOOLEAN, "BOOLEAN");
        assertPrimitiveMapping(Type.BYTES, "BYTEA");
        assertPrimitiveMapping(Type.STRING, "TEXT");
    }

    @Test
    public void bytesTest() throws Exception {
        String str = "\uD86F\uDE24";
        System.out.println(str);
        byte[] b = str.getBytes();
        System.out.println("Array " + b);
        System.out.println("Array as String" + Arrays.toString(b));
//        𫸤
//        Array [B@5d624da6
//        Array as String[-16, -85, -72, -92]
    }

    @Test
    public void bytesTest2() throws Exception {
//        String str = "\uD86F\uDE24";
//        byte[] b = str.getBytes();
//        byte[] b = BigInteger.valueOf(0x4e00).toByteArray();
//

//        ByteBuffer b = ByteBuffer.allocate(4);
//        b.putInt(0x4e00);
//
//        byte[] result = b.array();


        char[] result = Character.toChars(0x2BE24);

        String s = new String(result, 0, result.length);


        System.out.println(s);
        byte[] b = s.getBytes();
        System.out.println("Array " + b);
        System.out.println("Array as String" + Arrays.toString(b));
//        𫸤
//        Array [B@5d624da6
//        Array as String[-16, -85, -72, -92]
    }

    @Test
    public void run() throws Exception {
        // 获取所有中文（utf-8 中文编码范围：u4e00-u9fa5）
        int start;
        int end;

        start = Integer.parseInt("0000", 16);
//        start = Integer.parseInt("1000", 16);
        end = Integer.parseInt("10FFFF", 16);
        chineses(start, end);

//        start = Integer.parseInt("4E00", 16);
//        end = Integer.parseInt("9FBB", 16);
//        chineses(start, end);

//        start = Integer.parseInt("3400", 16);
//        end = Integer.parseInt("4DB5", 16);
//        chineses(start, end);
//
//        start = Integer.parseInt("20001", 16);
//        end = Integer.parseInt("2A6D6", 16);
//        chineses(start, end);
//
//        start = Integer.parseInt("F900", 16);
//        end = Integer.parseInt("FA2D", 16);
//        chineses(start, end);
//
//        start = Integer.parseInt("FA30", 16);
//        end = Integer.parseInt("FA6A", 16);
//        chineses(start, end);
//
//        start = Integer.parseInt("FA70", 16);
//        end = Integer.parseInt("FAD9", 16);
//        chineses(start, end);
//
//        start = Integer.parseInt("2F800", 16);
//        end = Integer.parseInt("2FA1D", 16);
//        chineses(start, end);

    }


    public void chineses(int start, int end) throws Exception {

        Map<String, String> properties = new HashMap<String, String>();

        properties.put(JdbcSourceConnectorConfig.TOPIC_PREFIX_CONFIG, "postgres");
        properties.put(JdbcSourceConnectorConfig.CONNECTION_URL_CONFIG, "jdbc:postgresql://node1:5432/aaaa");
        properties.put(JdbcSourceConnectorConfig.CONNECTION_USER_CONFIG, "postgres");
        properties.put(JdbcSourceConnectorConfig.CONNECTION_PASSWORD_CONFIG, "postgres");
        properties.put(JdbcSourceConnectorConfig.TABLE_WHITELIST_CONFIG, "aaaa");
        properties.put(JdbcSourceConnectorConfig.MODE_CONFIG, JdbcSourceConnectorConfig.MODE_BULK);


        JdbcSourceConnectorConfig config = new JdbcSourceConnectorConfig(properties);
        dialect = (PostgreSqlDatabaseDialect) DatabaseDialects.findBestFor(config.getString(JdbcSourceConnectorConfig.CONNECTION_URL_CONFIG), config);
        System.out.println(config);
        final int maxConnAttempts = config.getInt(JdbcSourceConnectorConfig.CONNECTION_ATTEMPTS_CONFIG);
        final long retryBackoff = config.getLong(JdbcSourceConnectorConfig.CONNECTION_BACKOFF_CONFIG);
        CachedConnectionProvider cachedConnectionProvider = new CachedConnectionProvider(dialect, maxConnAttempts, retryBackoff);


        PreparedStatement stmt;
        String queryStr =
                dialect.expressionBuilder().append("SELECT * FROM aaaa").toString();
        stmt = dialect.createPreparedStatement(cachedConnectionProvider.getConnection(), queryStr);

//        ResultSet resultSet = stmt.executeQuery();
//        System.out.println(resultSet);
//        while (resultSet.next()) {
//            System.out.printf("%-30.30s  %-30.30s%n", resultSet.getString("a"), resultSet.getString("b"));
//        }
//        Statement stmnt = null;
//        stmnt = cachedConnectionProvider.getConnection().createStatement();


        PreparedStatement ps = null;

        String sql = "INSERT INTO aaaa (b,c,d) "
                + "VALUES " + "(?,?,?)";

        ps = cachedConnectionProvider.getConnection().prepareStatement(sql);

        int insertCount = 0;

        ps.executeBatch();
        StringBuilder sb = new StringBuilder();
        final int batchSize = 100000;
        for (int i = start; i <= end; i++) {

            System.out.println(i);
//            char a = (char) i;
            String toHexString = Integer.toHexString(i);
            final char[] chars = Character.toChars(i);
//            String s = String.valueOf(chars);
            String s = new String(chars, 0, chars.length);

//            System.out.println(s);
            //
            if (chars[0] != '\0' && s != null && !s.equals("")) {

//                final String s = new String(chars);
                ps.setString(1, s);
//                ps.setString(1, (char) i + "");
                ps.setString(2, toHexString);
                ps.setInt(3, i);
                ps.addBatch();
                if (++insertCount % batchSize == 0) {
                    ps.executeBatch();
                }
            }


        }
        ps.executeBatch();
        ps.close();
        cachedConnectionProvider.getConnection().close();
        // 将文字写入文本中
//        File file = new File("chinese.txt");
//        file.createNewFile();
//        FileWriter writer = new FileWriter(file);
//        writer.write(sb.toString());
//        writer.close();
//        System.out.println("文字已写入当前工程Chinese.txt中，请刷新工程查看。");
    }

    public static boolean isUTF8(String key) {
        try {
            key.getBytes("utf-8");
            return true;
        } catch (UnsupportedEncodingException e) {
            return false;
        }
    }

    @Test
    public void shouldMapDecimalSchemaTypeToDecimalSqlType() {
        assertDecimalMapping(0, "DECIMAL");
        assertDecimalMapping(3, "DECIMAL");
        assertDecimalMapping(4, "DECIMAL");
        assertDecimalMapping(5, "DECIMAL");
    }

    @Test
    public void shouldMapDataTypes() {
        verifyDataTypeMapping("SMALLINT", Schema.INT8_SCHEMA);
        verifyDataTypeMapping("SMALLINT", Schema.INT16_SCHEMA);
        verifyDataTypeMapping("INT", Schema.INT32_SCHEMA);
        verifyDataTypeMapping("BIGINT", Schema.INT64_SCHEMA);
        verifyDataTypeMapping("REAL", Schema.FLOAT32_SCHEMA);
        verifyDataTypeMapping("DOUBLE PRECISION", Schema.FLOAT64_SCHEMA);
        verifyDataTypeMapping("BOOLEAN", Schema.BOOLEAN_SCHEMA);
        verifyDataTypeMapping("TEXT", Schema.STRING_SCHEMA);
        verifyDataTypeMapping("BYTEA", Schema.BYTES_SCHEMA);
        verifyDataTypeMapping("DECIMAL", Decimal.schema(0));
        verifyDataTypeMapping("DATE", Date.SCHEMA);
        verifyDataTypeMapping("TIME", Time.SCHEMA);
        verifyDataTypeMapping("TIMESTAMP", Timestamp.SCHEMA);
    }

    @Test
    public void shouldMapDateSchemaTypeToDateSqlType() {
        assertDateMapping("DATE");
    }

    @Test
    public void shouldMapTimeSchemaTypeToTimeSqlType() {
        assertTimeMapping("TIME");
    }

    @Test
    public void shouldMapTimestampSchemaTypeToTimestampSqlType() {
        assertTimestampMapping("TIMESTAMP");
    }

    @Test
    public void shouldBuildCreateQueryStatement() {
        assertEquals(
                "CREATE TABLE \"myTable\" (\n"
                        + "\"c1\" INT NOT NULL,\n"
                        + "\"c2\" BIGINT NOT NULL,\n"
                        + "\"c3\" TEXT NOT NULL,\n"
                        + "\"c4\" TEXT NULL,\n"
                        + "\"c5\" DATE DEFAULT '2001-03-15',\n"
                        + "\"c6\" TIME DEFAULT '00:00:00.000',\n"
                        + "\"c7\" TIMESTAMP DEFAULT '2001-03-15 00:00:00.000',\n"
                        + "\"c8\" DECIMAL NULL,\n"
                        + "PRIMARY KEY(\"c1\"))",
                dialect.buildCreateTableStatement(tableId, sinkRecordFields)
        );

        quoteIdentfiiers = QuoteMethod.NEVER;
        dialect = createDialect();

        assertEquals(
                "CREATE TABLE myTable (\n"
                        + "c1 INT NOT NULL,\n"
                        + "c2 BIGINT NOT NULL,\n"
                        + "c3 TEXT NOT NULL,\n"
                        + "c4 TEXT NULL,\n"
                        + "c5 DATE DEFAULT '2001-03-15',\n"
                        + "c6 TIME DEFAULT '00:00:00.000',\n"
                        + "c7 TIMESTAMP DEFAULT '2001-03-15 00:00:00.000',\n"
                        + "c8 DECIMAL NULL,\n"
                        + "PRIMARY KEY(c1))",
                dialect.buildCreateTableStatement(tableId, sinkRecordFields)
        );
    }

    @Test
    public void shouldBuildAlterTableStatement() {
        assertEquals(
                Arrays.asList(
                        "ALTER TABLE \"myTable\" \n"
                                + "ADD \"c1\" INT NOT NULL,\n"
                                + "ADD \"c2\" BIGINT NOT NULL,\n"
                                + "ADD \"c3\" TEXT NOT NULL,\n"
                                + "ADD \"c4\" TEXT NULL,\n"
                                + "ADD \"c5\" DATE DEFAULT '2001-03-15',\n"
                                + "ADD \"c6\" TIME DEFAULT '00:00:00.000',\n"
                                + "ADD \"c7\" TIMESTAMP DEFAULT '2001-03-15 00:00:00.000',\n"
                                + "ADD \"c8\" DECIMAL NULL"
                ),
                dialect.buildAlterTable(tableId, sinkRecordFields)
        );

        quoteIdentfiiers = QuoteMethod.NEVER;
        dialect = createDialect();

        assertEquals(
                Arrays.asList(
                        "ALTER TABLE myTable \n"
                                + "ADD c1 INT NOT NULL,\n"
                                + "ADD c2 BIGINT NOT NULL,\n"
                                + "ADD c3 TEXT NOT NULL,\n"
                                + "ADD c4 TEXT NULL,\n"
                                + "ADD c5 DATE DEFAULT '2001-03-15',\n"
                                + "ADD c6 TIME DEFAULT '00:00:00.000',\n"
                                + "ADD c7 TIMESTAMP DEFAULT '2001-03-15 00:00:00.000',\n"
                                + "ADD c8 DECIMAL NULL"
                ),
                dialect.buildAlterTable(tableId, sinkRecordFields)
        );
    }

    @Test
    public void shouldBuildUpsertStatement() {
        assertEquals(
                "INSERT INTO \"myTable\" (\"id1\",\"id2\",\"columnA\",\"columnB\"," +
                        "\"columnC\",\"columnD\") VALUES (?,?,?,?,?,?) ON CONFLICT (\"id1\"," +
                        "\"id2\") DO UPDATE SET \"columnA\"=EXCLUDED" +
                        ".\"columnA\",\"columnB\"=EXCLUDED.\"columnB\",\"columnC\"=EXCLUDED" +
                        ".\"columnC\",\"columnD\"=EXCLUDED.\"columnD\"",
                dialect.buildUpsertQueryStatement(tableId, pkColumns, columnsAtoD)
        );

        quoteIdentfiiers = QuoteMethod.NEVER;
        dialect = createDialect();

        assertEquals(
                "INSERT INTO myTable (id1,id2,columnA,columnB," +
                        "columnC,columnD) VALUES (?,?,?,?,?,?) ON CONFLICT (id1," +
                        "id2) DO UPDATE SET columnA=EXCLUDED" +
                        ".columnA,columnB=EXCLUDED.columnB,columnC=EXCLUDED" +
                        ".columnC,columnD=EXCLUDED.columnD",
                dialect.buildUpsertQueryStatement(tableId, pkColumns, columnsAtoD)
        );
    }

    @Test
    public void createOneColNoPk() {
        verifyCreateOneColNoPk(
                "CREATE TABLE \"myTable\" (" + System.lineSeparator() + "\"col1\" INT NOT NULL)");
    }

    @Test
    public void createOneColOnePk() {
        verifyCreateOneColOnePk(
                "CREATE TABLE \"myTable\" (" + System.lineSeparator() + "\"pk1\" INT NOT NULL," +
                        System.lineSeparator() + "PRIMARY KEY(\"pk1\"))");
    }

    @Test
    public void createThreeColTwoPk() {
        verifyCreateThreeColTwoPk(
                "CREATE TABLE \"myTable\" (" + System.lineSeparator() + "\"pk1\" INT NOT NULL," +
                        System.lineSeparator() + "\"pk2\" INT NOT NULL," + System.lineSeparator() +
                        "\"col1\" INT NOT NULL," + System.lineSeparator() + "PRIMARY KEY(\"pk1\",\"pk2\"))");

        quoteIdentfiiers = QuoteMethod.NEVER;
        dialect = createDialect();

        verifyCreateThreeColTwoPk(
                "CREATE TABLE myTable (" + System.lineSeparator() + "pk1 INT NOT NULL," +
                        System.lineSeparator() + "pk2 INT NOT NULL," + System.lineSeparator() +
                        "col1 INT NOT NULL," + System.lineSeparator() + "PRIMARY KEY(pk1,pk2))");
    }

    @Test
    public void alterAddOneCol() {
        verifyAlterAddOneCol("ALTER TABLE \"myTable\" ADD \"newcol1\" INT NULL");
    }

    @Test
    public void alterAddTwoCol() {
        verifyAlterAddTwoCols(
                "ALTER TABLE \"myTable\" " + System.lineSeparator() + "ADD \"newcol1\" INT NULL," +
                        System.lineSeparator() + "ADD \"newcol2\" INT DEFAULT 42");
    }

    @Test
    public void upsert() {
        TableId customer = tableId("Customer");
        assertEquals(
                "INSERT INTO \"Customer\" (\"id\",\"name\",\"salary\",\"address\") " +
                        "VALUES (?,?,?,?) ON CONFLICT (\"id\") DO UPDATE SET \"name\"=EXCLUDED.\"name\"," +
                        "\"salary\"=EXCLUDED.\"salary\",\"address\"=EXCLUDED.\"address\"",
                dialect.buildUpsertQueryStatement(
                        customer,
                        columns(customer, "id"),
                        columns(customer, "name", "salary", "address")
                )
        );

        quoteIdentfiiers = QuoteMethod.NEVER;
        dialect = createDialect();

        assertEquals(
                "INSERT INTO Customer (id,name,salary,address) " +
                        "VALUES (?,?,?,?) ON CONFLICT (id) DO UPDATE SET name=EXCLUDED.name," +
                        "salary=EXCLUDED.salary,address=EXCLUDED.address",
                dialect.buildUpsertQueryStatement(
                        customer,
                        columns(customer, "id"),
                        columns(customer, "name", "salary", "address")
                )
        );
    }

    @Test
    public void shouldSanitizeUrlWithoutCredentialsInProperties() {
        assertSanitizedUrl(
                "jdbc:postgresql://localhost/test?user=fred&ssl=true",
                "jdbc:postgresql://localhost/test?user=fred&ssl=true"
        );
    }

    @Test
    public void shouldSanitizeUrlWithCredentialsInUrlProperties() {
        assertSanitizedUrl(
                "jdbc:postgresql://localhost/test?user=fred&password=secret&ssl=true",
                "jdbc:postgresql://localhost/test?user=fred&password=****&ssl=true"
        );
    }
}