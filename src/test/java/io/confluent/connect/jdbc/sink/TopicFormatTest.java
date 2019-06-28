package io.confluent.connect.jdbc.sink;

import org.junit.Test;

/**
 * TopicFormatTest
 */
public class TopicFormatTest {

    @Test
    public void replace() {
        final String tableName = "table1".replace("${topic}", "yz-table1");
        final String tableName2 = "yz-table2".replace("yz-", "");
        System.out.println(tableName);
        System.out.println(tableName2);
    }
}