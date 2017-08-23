package com.epam.hive;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public interface QueryForTestStatements {
    String CREATE_NON_PARTITION_TABLE = "CREATE TABLE IF NOT EXISTS test_fortest(userid BIGINT, view_time STRING,\n" +
            "page_url STRING, country STRING)\n" +
            "COMMENT 'This is the page view table'\n" +
            "ROW FORMAT DELIMITED \n" +
            "FIELDS TERMINATED BY '\\n'\n" +
            "STORED AS SEQUENCEFILE";
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
    String INSERT_INTO_LOCAL_FILE = "INSERT OVERWRITE DIRECTORY '/wordcount/task/fortest'\n" +
            "SELECT test_fortest.*\n" +
            "FROM test_fortest";
    String INSERT_INTO_ANOTHER_TABLE = "FROM test_fortest\n" +
            "INSERT OVERWRITE TABLE test_test PARTITION(dt, country)\n" +
            "SELECT test_fortest.view_time, test_fortest.userid, test_fortest.page_url, null, null, to_date(test_fortest.view_time) dt, test_fortest.country";
    String INSERT_FIRST_PART_STATEMENT = "INSERT INTO TABLE";
    String INSERT_PARTITION_PART_STATEMENT = "PARTITION";
    String INSERT_VALUES_PART_STATEMENT = "VALUES";
    String DROP_TABLE = "DROP TABLE IF EXISTS";
}
