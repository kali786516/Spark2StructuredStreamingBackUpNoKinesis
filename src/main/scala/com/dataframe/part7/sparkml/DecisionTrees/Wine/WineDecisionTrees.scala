package com.dataframe.part7.sparkml.DecisionTrees.Wine

/**
  * Created by kalit_000 on 5/28/19.
  */

import org.apache.spark.SparkContext
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import scala.io.Source
import com.dataframe.part7.sparkml.DecisionTrees.BikeBuyerADW.common.PrintUtils.printMetrics
import com.dataframe.part7.sparkml.DecisionTrees.BikeBuyerADW.common.Stats
import com.dataframe.part7.sparkml.DecisionTrees.BikeBuyerADW.common.Stats.confusionMatrix
import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.tree.model.DecisionTreeModel
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.util.MLUtils


object WineDecisionTrees {

  def main(args: Array[String]): Unit = {

    //org.apache.log4j.PropertyConfigurator.configure(Thread.currentThread().getContextClassLoader().getResourceAsStream("log4j.config"))
    val logger = Logger.getLogger("BikeBuyersDecisionTrees")
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

    val spark = SparkSession.builder().appName("Classification of Bike Buyers with DecisionTree").master("local[*]").getOrCreate()
    val sc = spark.sparkContext

    val bbFile = sc.textFile(args.headOption.getOrElse("sparkMLDataSets/") + "wine.data")

    val data = bbFile.map { row =>
      WineModel(row.split(",")).toLabeledPoint
    }

    data.take(5).foreach(println)

    /*
    convert data to LibSVM
    https://stackoverflow.com/questions/43920111/convert-dataframe-to-libsvm-format
    */
    //MLUtils.saveAsLibSVMFile(data,"sparkml-libsvm-op-folder/WineDecisionTreesLibSVM")

    /* read libsvm LibSVM*/
    /*val df = spark.read.format("libsvm")
      .option("numFeatures", "780")
      .load("sparkml-libsvm-op-folder/WineDecisionTreesLibSVM")

    df.show(10)*/

    val Array(train, test) = data.randomSplit(Array(.9, .1), 102059L)
    train.cache()
    test.cache()

    /* in the wine data set all input feature values are numeric so we don't need to pass categorical value*/
    /*com.dataframe.part7.sparkml.DecisionTrees.BikeBuyer.categoricalFeaturesInfo*/
    val categoricalFeaturesInfo = Map[Int,Int]()

    /* numClasses represents how many categories our train data contains (Bike Buyer 1 or 0) in our case */
    /* numClasses should be 1+ max(categories label) for bike buyer its 1 so 1+1 = 2 */

    val dtree = DecisionTree.trainClassifier(train, 4, categoricalFeaturesInfo, "gini", 3, 32)


    test.take(5).foreach {
      x => println(s"Predicted: ${dtree.predict(x.features)}, Label: ${x.label}")
    }

    /* dont pass in label as per Jahnavi Ravi Spark Course (https://app.pluralsight.com/player?course=spark-2-building-machine-learning-models&author=janani-ravi&name=f5057401-e012-494f-97c3-cdc30f3d3e7d&clip=9&mode=live) */
    test.take(5).foreach {
      x => println(s"Predicted: ${dtree.predict(x.features)}")
    }

    val predictionsAndLabels = test.map {
      point => (dtree.predict(point.features), point.label)
    }

    val sparkMlMetrics = new MulticlassMetrics(predictionsAndLabels)

    println("Spark ML Metric Accuracy:-"+sparkMlMetrics.accuracy)

    println("Spark ML Confusion Matrix:- "+sparkMlMetrics.confusionMatrix)

    println("Spark ML Model Trees:-"+dtree.toDebugString)

    val stats = Stats(confusionMatrix(predictionsAndLabels))
    println(stats.toString)

    val metrics = new BinaryClassificationMetrics(predictionsAndLabels)
    printMetrics(metrics)

    println("Spark ML Model Trees:-"+dtree.toDebugString)

    spark.stop()


  }

}
