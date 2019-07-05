package io.confluent.connect.jdbc.sink;

import org.junit.Test;

/**
 * TopicFormatTest
 */
public class TopicFormatTest {

    @Test
    public void replace() {
        final String tableName = "table1".replace("${topic}", "yz-table1");
        final String tableName2 = "yz-tabyz-le2yz-".replace("yz-{0}", "");
        System.out.println(tableName);
        System.out.println(tableName2);
    }
}
