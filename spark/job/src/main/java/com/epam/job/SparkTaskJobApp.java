package com.epam.job;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

public class SparkTaskJobApp {
    public static void main(String[] args){
        SparkConf conf = new SparkConf().setAppName("wordcount");
        JavaSparkContext sc = new JavaSparkContext(conf);

        sc.textFile("/wordcount/input/page5000.txt").map(s -> Arrays.asList(s.replaceAll("\\p{Punct}", " ").trim().toUpperCase().split(" ")).iterator())
                .mapToPair(s -> new Tuple2<>(s, 1)).reduceByKey(Integer::sum).saveAsTextFile("/wordcount/task/result.txt");
    }
}
