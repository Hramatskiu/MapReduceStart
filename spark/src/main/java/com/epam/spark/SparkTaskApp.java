package com.epam.spark;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.deploy.Client;
import org.apache.spark.launcher.SparkLauncher;
import scala.Tuple2;

public class SparkTaskApp {
    public static void main(String[] args){
        SparkConf conf = new SparkConf().setAppName("wordcount").setMaster("yarn");
        JavaSparkContext sc = new JavaSparkContext(conf);

        sc.textFile("/wordcount/input/fortest.txt").mapToPair(s -> new Tuple2<>(s, 1))
                .reduceByKey(Integer::sum).saveAsTextFile("/wordcount/task/result.txt");

        SparkLauncher sparkLauncher = new SparkLauncher();
        sparkLauncher.setPropertiesFile("spark-default.properties").setAppName("wordcount").addJar("mapreduce-start-spark-1.0-SNAPSHOT.jar")
                .setMainClass("com.epam.spark.AparkTaskApp");
    }

    private static Configuration createConfig(){
        Configuration configuration = new Configuration();

        configuration.addResource("yarn-site.xml");
        configuration.addResource("hdfs-site.xml");
        configuration.addResource("core-site.xml");
        configuration.addResource("mapred-site.xml");
        configuration.addResource("ssl-client.xml");
        configuration.addResource("spark-defaults-metrics.properties");

        return configuration;
    }
}
