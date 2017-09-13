package com.epam.sqoop.creator;

import java.util.HashMap;
import java.util.Map;

public class SqoopConfigsCreator {
  public static Map<String, String> createConfigMapForJDBCConnector( String connectionString, String jdbcDriver,
                                                                     String username, String password ) {
    Map<String, String> configMap = new HashMap<>();

    configMap.put( "linkConfig.connectionString", connectionString );
    configMap.put( "linkConfig.jdbcDriver", jdbcDriver );
    configMap.put( "linkConfig.username", username );
    configMap.put( "linkConfig.password", password );

    return configMap;
  }

  public static Map<String, String> createConfigMapForHDFSConnector( String connectionUri ) {
    Map<String, String> configMap = new HashMap<>();

    configMap.put( "linkConfig.connectionUri", connectionUri );

    return configMap;
  }

  public static Map<String, String> createJobFromTableConfig( String shemaName, String tableName,
                                                              String partitionColumn ) {
    Map<String, String> configMap = new HashMap<>();

    configMap.put( "fromJobConfig.schemaName", shemaName );
    configMap.put( "fromJobConfig.tableName", tableName );
    configMap.put( "fromJobConfig.partitionColumn", partitionColumn );

    return configMap;
  }

  public static Map<String, String> createJobOtHDFSConfig( String outputDirectory ) {
    Map<String, String> configMap = new HashMap<>();

    configMap.put( "toJobConfig.outputDirectory", outputDirectory );

    return configMap;
  }

  public static Map<String, String> createJobDriverConfig( int numExtractors ) {
    Map<String, String> configMap = new HashMap<>();

    configMap.put( "throttlingConfig.numExtractors", String.valueOf( numExtractors ) );

    return configMap;
  }
}
