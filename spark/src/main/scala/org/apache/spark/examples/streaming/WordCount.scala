package org.apache.spark.examples.streaming

import org.apache.spark.sql.SparkSession

/**
  * Created by zhengqh on 17/5/22.
  */
object WordCount {

  def main(args: Array[String]) {
    val spark = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .master("local")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()

    // For implicit conversions like converting RDDs to DataFrames
    import spark.implicits._

    val sc = spark.sparkContext

    val line1 = "hello spark hello kafka"
    val line2 = "spark streaming kafka"

    val rdd = sc.parallelize(line1.split(" "))
    val wordCount = rdd.map(word => (word, 1)).reduceByKey(_ + _)
    wordCount.foreach(println)

    val rdd2 = sc.parallelize(List(line1, line2))
    val wordCount2 = rdd2.flatMap(line => line.split(" ")).map(word => (word, 1)).reduceByKey(_ + _)
    wordCount2.foreach(println)

  }
}
