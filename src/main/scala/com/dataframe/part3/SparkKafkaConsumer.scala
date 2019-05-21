package com.dataframe.part3

/**
  * Created by kalit_000 on 5/17/19.
  */
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.types._
import java.sql.Date
import java.text.SimpleDateFormat
import java.util.Calendar
object SparkKafkaConsumer {
  def main(args: Array[String]): Unit = {
    val logger = Logger.getLogger("HbIngestion")
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)
    val startTimeMillis = System.currentTimeMillis()

    val spark = SparkSession.builder
      .master("local[3]")
      .appName("CSVFileStream")
      .config("spark.driver.memory","2g")
      .config("spark.cassandra.connection.host","localhost")
      .config("partition.assignment.strategy","range")
      .enableHiveSupport()
      .getOrCreate()

    import spark.implicits._

    val kafkaStream = spark.readStream.format("kafka").option("kafka.bootstrap.servers","localhost:2181")
                           .option("subscribe","spark_avro_topic")
                           .option("startingOffsets","latest")
                           .option("partition.assignment.strategy","range")
                           .load

    val kafkaDF = kafkaStream.selectExpr("CAST(value as STRING) as message")

    //kafkaDF.show(false)

    val query = kafkaDF.writeStream.outputMode("append")
                       .format("console")
                       .option("truncate","false")
                       .trigger(Trigger.ProcessingTime("5 seconds"))
                       .start()
                       .awaitTermination()



  }

}
