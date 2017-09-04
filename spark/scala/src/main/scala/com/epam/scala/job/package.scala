package com.epam.scala

import org.apache.spark.{SparkConf, SparkContext}

package object SparkTaskJobApp {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName("Spark Count"))
    sc.textFile("/wordcount/input/page5000.txt").flatMap(_.replaceAll("\\p{Punct}", " ").trim().toUpperCase().split(" ")).map((_, 1))
      .reduceByKey(_ + _).saveAsTextFile("/wordcount/task/result.txt")
  }
}
