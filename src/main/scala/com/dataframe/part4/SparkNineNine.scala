package com.dataframe.part4

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType

/**
  * Created by kalit_000 on 5/21/19.
  */
object SparkNineNine {
  def main(args: Array[String]): Unit = {
    val logger = Logger.getLogger("HbIngestion")
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)
    val startTimeMillis = System.currentTimeMillis()

    val spark = SparkSession.builder
      .master("local[*]")
      .appName("CSVFileStream")
      //.config("spark.driver.memory","2g")
      //.config("spark.cassandra.connection.host","localhost")
      .enableHiveSupport()
      .getOrCreate()

    import spark.implicits._

    val schemaUntyped = new StructType()
      .add("lsoa_code", "string")
      .add("borough", "string")
      .add("major_category", "string")
      .add("minor_category", "string")
      .add("value", "string")
      .add("year", "string")
      .add("month", "string")

    val data = List(1,1,1,2,2,2,3,4,5)

    val rdd = spark.sparkContext.parallelize(data)

    println(rdd.map(x => (x,1)).countByKey().toList)

    //println(rdd.map(x => (x,1)).groupByKey().map(x => (x._1,x._2.sum)).collect())

    val words = Array("one", "two", "two", "three", "three", "three")
    val wordPairsRDD = spark.sparkContext.parallelize(words).map(word => (word, 1))

    /*
    val wordCountsWithGroup = wordPairsRDD
      .groupByKey()
      .map(t => (t._1, t._2.sum))
      .collect()*/

    //wordCountsWithGroup.map(x => println(x))

    val x = Array(Array(1,2),Array(3,4),Array(5,6),Array(7,8),Array(9,10),Array(11,12),Array(13,14),Array(15,16),Array(17,18),Array(19,20),Array(21,22))

    x.map(x => println(x))

    x.foreach(println)

  }

}
