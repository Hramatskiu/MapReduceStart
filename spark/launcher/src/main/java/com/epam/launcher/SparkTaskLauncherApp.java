package com.epam.launcher;

import org.apache.spark.launcher.SparkLauncher;

public class SparkTaskLauncherApp {
  public static void main( String[] args ) throws Exception {
    SparkLauncher sparkLauncher = new SparkLauncher();
    sparkLauncher.setPropertiesFile( "spark-default.properties" ).setAppName( "wordcount" )
      .addJar( "mapreduce-start-spark-job-1.0-SNAPSHOT.jar" )
      .setMainClass( "com.epam.job.SparkTaskJobApp" ).setMaster( "http://svqxbdcn6hdp25n2.pentahoqa.com:8050" )
      .setDeployMode( "cluster" );
    sparkLauncher.launch().waitFor();
  }
}
