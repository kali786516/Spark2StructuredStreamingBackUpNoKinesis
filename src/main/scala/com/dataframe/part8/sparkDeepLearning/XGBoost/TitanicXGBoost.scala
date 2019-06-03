package com.dataframe.part8.sparkDeepLearning.XGBoost

import com.google.common.collect.ImmutableMap
import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler}
import org.apache.spark.sql.SparkSession
import ml.dmlc.xgboost4j.scala.spark._
import ml.dmlc.xgboost4j.scala.spark._
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import scala.collection.mutable
import org.apache.spark.ml.Pipeline
import ml.dmlc.xgboost4j.scala.spark.XGBoostClassificationModel
import org.apache.spark.ml.tuning._
import org.apache.spark.ml.PipelineModel
import ml.dmlc.xgboost4j.scala.spark.XGBoostClassificationModel
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
/**
  * Created by kalit_000 on 5/30/19.
  */
object TitanicXGBoost {

  def main(args: Array[String]): Unit = {

    val logger = Logger.getLogger("BikeBuyersDecisionTrees")
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

    val spark = SparkSession.builder().appName("Examine data about passensgers on the titanic").master("local[*]").getOrCreate()
    val sc = spark.sparkContext

    val rawData=spark.read.format("csv")
      .option("header","true")
      .load("sparkMLDataSets/titanic.csv")

    println("-" * 100)

    println("Sample Data 10 Records")
    rawData.show(10)

    println(rawData.count())

    println("-" * 100)

    println("cast data sets")

    rawData.createOrReplaceTempView("rawDataTable")

    spark.sql("select cast(Survived as double) as Survived ,cast(Pclass as float) as Pclass," +
      "Sex,cast(Age as double) as Age ,cast(Fare as double) as Fare," +
      "Embarked,Cabin from rawDataTable").show(10,false)

    val dataSet = spark.sql("select cast(Survived as double) as Survived ,cast(Pclass as float) as Pclass," +
      "Sex,cast(Age as double) as Age ,cast(Fare as double) as Fare," +
      "Embarked,Cabin from rawDataTable")

    val cleanedDataSet=dataSet.na.replace("*", ImmutableMap.of("?", "null")).na.drop()

    cleanedDataSet.show(10)

    println(cleanedDataSet.count())

    println("String Indexer Step 1 ....................................................")

    /*
    val sexIndexer = new StringIndexer().setInputCol("Sex").setOutputCol("Sex" + "_indexed").fit(cleanedDataSet)

    val cabinIndexer = new StringIndexer().setInputCol("Cabin").setOutputCol("Cabin" + "_indexed").fit(cleanedDataSet)

    val embarkedIndexer = new StringIndexer().setInputCol("Embarked").setOutputCol("Embarked" + "_indexed").fit(cleanedDataSet)
    */

    val stringIndexColumns = Seq("Sex","Embarked","Cabin")

    val stringIndexer = stringIndexColumns.map { colName =>
      new StringIndexer().setInputCol(colName).setOutputCol(colName + "_indexed").fit(cleanedDataSet)
    }

    val pipeline = new Pipeline().setStages(stringIndexer.toArray)

    val StringIndexedDF = pipeline.fit(cleanedDataSet).transform(cleanedDataSet)

    print("-" * 100)

    print("Data Set After StringIndexer addinging _indexex")

    StringIndexedDF.show(10)

    val opDF = StringIndexedDF.drop("Sex","Embarked").withColumnRenamed("Sex_indexed","Gender").withColumnRenamed("Embarked_indexed","Boarded")

    println("Vector Assembler Step  2....................................................")

    val assembler = new VectorAssembler()
      .setInputCols(Array("Survived","Pclass","Age","Fare","Gender","Cabin_indexed","Boarded"))
      .setOutputCol("features")

    val assemberDF = assembler.transform(opDF)

    val Array(traindata,testdata) = assemberDF.randomSplit(Array(.8, .2))

    println("Define XGBoost Estimator Step  3....................................................")

    def get_param(): mutable.HashMap[String, Any] = {
      val params = new mutable.HashMap[String, Any]()
      params += "eta" -> 0.1
      params += "max_depth" -> 3
      params += "min_child_weight" -> 1
      params += "num_rounds" -> 10
      params += "silent" -> 0
      params += "nthread" -> 1
      params += "objective" -> "binary:logistic"
      //params += "objective" -> "reg:linear"
      params += "nWorkers" -> 1
      //      params += "booster" -> "gbtree"
      //      params += "gamma" -> 0.0
      //      params += "colsample_bylevel" -> 1
      return params
    }

    //val xgbEstimator = new XGBoostEstimator(get_param().toMap).setLabelCol("Survived").setPredictionCol("prediction").setFeaturesCol("features")
    val xgbEstimator = new XGBoostClassifier(get_param().toMap).setLabelCol("Survived").setPredictionCol("prediction").setFeaturesCol("features")

    // Chain indexers and tree in a Pipeline.
    val pipeline2 = new Pipeline().setStages(Array(xgbEstimator))

    // Train model. This also runs the indexers.
    val model = pipeline2.fit(traindata)

    // Make predictions.
    val predictions = model.transform(testdata)

    predictions.show(5,false)

    println("Evaluating XG Boost Step  4....................................................")

    val evaluator = new MulticlassClassificationEvaluator().setLabelCol("Survived").setPredictionCol("prediction").setMetricName("f1")
    val accuracy = evaluator.evaluate(predictions)

    println("XGBoost.trainWithDataFrame Test Error = " + (1.0 - accuracy))

    println(evaluator.setMetricName("accuracy").evaluate(predictions))


    //println("Hyper Tunning XG Boost Step  5....................................................")

    /*
    val paramGrid = new ParamGridBuilder()
      .addGrid(booster.maxDepth, Array(3, 8))
      .addGrid(booster.eta, Array(0.2, 0.6))
      .build()
    val cv = new CrossValidator()
      .setEstimator(pipeline)
      .setEvaluator(evaluator)
      .setEstimatorParamMaps(paramGrid)
      .setNumFolds(3)

    val cvModel = cv.fit(traindata)

    val bestModel = cvModel.bestModel.asInstanceOf[PipelineModel].stages(2)
      .asInstanceOf[XGBoostClassificationModel]
    bestModel.extractParamMap()
    */

    sc.stop()


  }

}
