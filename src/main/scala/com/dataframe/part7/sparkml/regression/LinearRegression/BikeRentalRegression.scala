package com.dataframe.part7.sparkml.regression.LinearRegression

import com.google.common.collect.ImmutableMap
import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.sql.SparkSession
import breeze.plot._

/**
  * Created by kalit_000 on 5/30/19.
  */
object BikeRentalRegression {

  def main(args: Array[String]): Unit = {

    val logger = Logger.getLogger("BikeBuyersDecisionTrees")
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

    val spark = SparkSession.builder().appName("Predict Bike Rentals (count) using LineraRegression").master("local[*]").getOrCreate()
    val sc = spark.sparkContext

    val rawData=spark.read.format("csv")
      .option("header","true")
      .load("sparkMLDataSets/day.csv")

    println("-" * 100)

    println("Sample Data 10 Records")
    rawData.show(10)

    println(rawData.count())

    println("-" * 100)

    println("cast data sets")

    rawData.createOrReplaceTempView("rawDataTable")

    val dataSet = spark.sql("select cast(season as float) as season ,cast(yr as float) as yr," +
      "cast(mnth as float) as mnth,cast(holiday as float) as holiday ,cast(weekday as float) as weekday,cast(workingday as float) as workingday" +
      ",cast(weathersit as float) as weathersit,cast(temp as float) as temp,cast(atemp as float) as atemp," +
      "cast(hum as float) as hum,cast(windspeed as float) as windspeed,cast(cnt as float) as cnt from rawDataTable")

    val cleanedDataSet=dataSet.na.replace("*", ImmutableMap.of("?", "null")).na.drop()

    cleanedDataSet.show(10)

    println(cleanedDataSet.count())

     println("Vector Assembler Step  1....................................................")

    val assembler = new VectorAssembler()
      .setInputCols(Array("season","yr","mnth","holiday", "weekday",
        "workingday","weathersit","temp","atemp","hum","windspeed"))
      .setOutputCol("features")

    val assemberDF = assembler.transform(cleanedDataSet)

    assemberDF.show(10)

    println("Linear Regression Step  2....................................................")

    val Array(traindata,testdata) = assemberDF.randomSplit(Array(.8, .2))

    val lr = new LinearRegression().setMaxIter(100).setRegParam(1.0).setElasticNetParam(0.8).setLabelCol("cnt").setFeaturesCol("features")

    val linearRegModel = lr.fit(traindata)

    println("R2 Score:-" + linearRegModel.summary.r2)
    println("Training RMSE:-" + linearRegModel.summary.rootMeanSquaredError)

    println("Predictions Step  3....................................................")

    val predictions = linearRegModel.transform(testdata)

    predictions.show(10)

    val evaluator = new RegressionEvaluator().setLabelCol("cnt").setPredictionCol("prediction").setMetricName("r2")

    val r2 = evaluator.evaluate(predictions)

    println("Test RMSE:-" + r2)

    sc.stop()

  }

}
