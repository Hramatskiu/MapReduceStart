package com.epam.hive;

public interface QueryForTestStatements {
    String CREATE_PARTITION_TABLE = "CREATE TABLE IF NOT EXISTS test_test(viewTime INT, userid BIGINT,\n" +
            "page_url STRING, referrer_url STRING,\n" +
            "ip STRING COMMENT 'IP Address of the User')\n" +
            "COMMENT 'This is the page view table'\n" +
            "PARTITIONED BY(dt STRING, country STRING)\n" +
            "ROW FORMAT DELIMITED\n" +
            "FIELDS TERMINATED BY '\\001'\n" +
            "STORED AS SEQUENCEFILE";
    String INSERT_DATA_PARTITION = "INSERT INTO TABLE test_test PARTITION ('17082017', 'Belarus')\n" +
            "VALUES (1, 1, 'some/url', 'awesome/url', '192.168.1.1'),\n" +
            "(5, 2, 'somet/url', 'awesomet/url', '192.168.1.2'),\n" +
            "(2, 3, 'tsome/url', 'tawesome/url', '192.168.1.3')";
}
