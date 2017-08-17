package com.epam.hive;

import java.io.FileInputStream;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;

public class HiveTaskApp {
    public static void main(String[] args) throws Exception{
        Properties properties = new Properties();
        properties.setProperty("user", "hive");
        properties.setProperty("password", "");
        //properties.loadFromXML(new FileInputStream("src/main/resources/hive-site.xml"));
        DriverConnectionHandler.registerDriver("org.apache.hive.jdbc.HiveDriver");
        ConnectionHandler connectionHandler = new ConnectionHandler("jdbc:hive2://svqxbdcn6hdp25n2.pentahoqa.com:10000/default;saslQop=auth", properties);

        connectionHandler.performTaskOnConnection(connection -> HiveQueryHelper.executeQuery(connection, QueryForTestStatements.CREATE_PARTITION_TABLE));
        connectionHandler.performTaskOnConnection(connection -> HiveQueryHelper.executeQuery(connection, QueryForTestStatements.INSERT_DATA_PARTITION));

        DriverConnectionHandler.deregisterAllDrivers();
    }
}
