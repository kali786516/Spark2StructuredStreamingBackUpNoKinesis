package com.dataframe.RealTimeFraudDetection.fraudDetection.spark.jobs

import com.dataframe.RealTimeFraudDetection.fraudDetection.spark.SparkConfig
import org.apache.spark.sql.SparkSession

/**
  * Created by kalit_000 on 6/1/19.
  */
abstract class SparkJob(appName:String) {

  lazy implicit val sparkSession = SparkSession.builder()
     .config(SparkConfig.sparkConf)
     .getOrCreate()

}
