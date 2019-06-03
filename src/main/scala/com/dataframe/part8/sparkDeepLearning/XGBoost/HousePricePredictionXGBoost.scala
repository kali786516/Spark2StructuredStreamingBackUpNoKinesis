package com.dataframe.part8.sparkDeepLearning.XGBoost

import com.google.common.collect.ImmutableMap
import ml.dmlc.xgboost4j.scala.spark._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.sql.SparkSession

import scala.collection.mutable

/**
  * Created by kalit_000 on 5/31/19.
  */
object HousePricePredictionXGBoost {

  def main(args: Array[String]): Unit = {

    val logger = Logger.getLogger("BikeBuyersDecisionTrees")
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

    val spark = SparkSession.builder().appName("Examine data about passensgers on the titanic").master("local[*]").getOrCreate()
    val sc = spark.sparkContext

    val rawData=spark.read.format("csv")
      .option("header","true")
      .load("sparkMLDataSets/house-data.csv")

    println("-" * 100)

    println("Sample Data 10 Records")
    rawData.show(10)

    println(rawData.count())

    println("-" * 100)

    println("cast data sets")

    rawData.createOrReplaceTempView("rawDataTable")

    spark.sql("select cast(price as double) as price ,cast(bedrooms as int) as bedrooms," +
      "cast(bathrooms as int) as bathrooms ,cast(sqft_living as int) as sqft_living," +
      "cast(sqft_lot as int) as sqft_lot,cast(floors as int) as floors,cast(waterfront as int) as waterfront," +
      "cast(view as int) as view,cast(condition as int) as condition,cast(grade as int) as grade," +
      "cast(sqft_above as int) as sqft_above,cast(sqft_basement as int) as sqft_basement," +
      "cast(yr_built as int) as yr_built,cast(yr_renovated as int) as yr_renovated,cast(zipcode as int) as zipcode," +
      "cast(lat as int) as lat,cast(long as int) as long," +
      "cast(sqft_living15 as int) as sqft_living15," +
      "cast(sqft_lot15 as int) as sqft_lot15 from rawDataTable").show(10,false)

    val dataSet = spark.sql("select cast(price as double) as price ,cast(bedrooms as int) as bedrooms," +
      "cast(bathrooms as int) as bathrooms ,cast(sqft_living as int) as sqft_living," +
      "cast(sqft_lot as int) as sqft_lot,cast(floors as int) as floors,cast(waterfront as int) as waterfront," +
      "cast(view as int) as view,cast(condition as int) as condition,cast(grade as int) as grade," +
      "cast(sqft_above as int) as sqft_above,cast(sqft_basement as int) as sqft_basement," +
      "cast(yr_built as int) as yr_built,cast(yr_renovated as int) as yr_renovated,cast(zipcode as int) as zipcode," +
      "cast(lat as int) as lat,cast(long as int) as long," +
      "cast(sqft_living15 as int) as sqft_living15," +
      "cast(sqft_lot15 as int) as sqft_lot15 from rawDataTable")

    val cleanedDataSet=dataSet.na.replace("*", ImmutableMap.of("?", "null")).na.drop()

    cleanedDataSet.show(10)

    println(cleanedDataSet.count())

    println("Vector Assembler Step  2....................................................")

    val assembler = new VectorAssembler()
      .setInputCols(Array("bedrooms","bathrooms","sqft_living","sqft_lot","floors","waterfront","view","condition","grade","sqft_above",
        "sqft_basement","yr_built","yr_renovated","zipcode","lat","long","sqft_living15","sqft_lot15"))
      .setOutputCol("features")

    val assemberDF = assembler.transform(cleanedDataSet)

    println("Split Data Step  3....................................................")

    val Array(traindata,testdata) = assemberDF.randomSplit(Array(.8, .2))

    println("Create Model Step  4....................................................")

    val reg = new LinearRegression().setLabelCol("price").setFeaturesCol("features")

    val model = reg.fit(traindata)

    println(model.coefficients)

    println("Predict Data Step  5....................................................")

    // Make predictions.
    val predictions = model.transform(testdata)

    predictions.show(5,false)

    println("Evaluate Step  6...................................................")

    val evaluator = new MulticlassClassificationEvaluator().setLabelCol("price").setPredictionCol("prediction")
    val accuracy = evaluator.evaluate(predictions)

    println("Accuracy :- "+ accuracy)

    println("Define XGBoost Estimator Step  7 ...................................................")

    def get_param(): mutable.HashMap[String, Any] = {
      val params = new mutable.HashMap[String, Any]()
      params += "eta" -> 0.3
      params += "max_depth" -> 6
      params += "num_rounds" -> 10
      params += "objective" -> "reg:linear"
      params += "nWorkers" -> 2
      return params
    }

    val xgboostReg = new XGBoostRegressor(get_param().toMap).setLabelCol("price").setFeaturesCol("features")

    //val xgboostReg = new XGBoostEstimator(get_param().toMap).setLabelCol("price").setPredictionCol("prediction").setFeaturesCol("features")

    // Chain indexers and tree in a Pipeline.
    val pipeline2 = new Pipeline().setStages(Array(xgboostReg))

    // Train model. This also runs the indexers.
    val model2 = pipeline2.fit(traindata)

    // Make predictions.
    val predictions2 = model2.transform(testdata)

    predictions2.show(5,false)

    println("Evaluating XG Boost Step  8....................................................")

    val evaluator2 = new MulticlassClassificationEvaluator().setLabelCol("price").setPredictionCol("prediction")
    val accuracy2 = evaluator2.evaluate(predictions)

    println("XGBoost.trainWithDataFrame Test Error = " + (1.0 - accuracy2))

    sc.stop()


  }

}
