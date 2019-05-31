package com.dataframe.part7.sparkml.DecisionTrees.BikeBuyerADW

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

object BikeBuyersDecisionTrees {

  def main(args: Array[String]): Unit = {

    //org.apache.log4j.PropertyConfigurator.configure(Thread.currentThread().getContextClassLoader().getResourceAsStream("log4j.config"))
    val logger = Logger.getLogger("BikeBuyersDecisionTrees")
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

    val spark = SparkSession.builder().appName("Classification of Bike Buyers with DecisionTree").master("local[*]").getOrCreate()
    val sc = spark.sparkContext

    val bbFile = sc.textFile(args.headOption.getOrElse("sparkMLDataSets/") + "bike-buyers.txt")

    val data = bbFile.map { row =>
      BikeBuyerModel(row.split("\\t")).toLabeledPoint
    }

    val Array(train, test) = data.randomSplit(Array(.9, .1), 102059L)
    train.cache()
    test.cache()

    /* numClasses represents how many categories our train data contains (Bike Buyer 1 or 0) in our case */
    /* numClasses should be 1+ max(categories label) for bike buyer its 1 so 1+1 = 2 */
    val numClasses = 2
    val impurity = "entropy"
    val maxDepth = 20
    val maxBins = 34

    /*

    */
    val dtree = DecisionTree.trainClassifier(train, numClasses, BikeBuyerModel.categoricalFeaturesInfo(), impurity, maxDepth, maxBins)

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

    /* Test Accuuracy of model */

    val testAcc = predictionsAndLabels.filter(x => (x._1 == x._2)).count() / test.count()
    println("Test Accuracy :- "+testAcc)

    val stats = Stats(confusionMatrix(predictionsAndLabels))
    println(stats.toString)

    val metrics = new BinaryClassificationMetrics(predictionsAndLabels)
    printMetrics(metrics)

    val sparkMlMetrics = new MulticlassMetrics(predictionsAndLabels)

    println("Spark ML Metric Accuracy:-"+sparkMlMetrics.accuracy)

    println("Spark ML Confusion Matrix:- "+sparkMlMetrics.confusionMatrix)

    println("Spark ML Model Trees:-"+dtree.toDebugString)

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

    /*
        Briefly, feature is input; label is output.
        A feature is one column of the data in your input set. For instance,
        if you're trying to predict the type of pet someone will choose,
        your input features might include age, home region, family income, etc.
        The label is the final choice, such as dog, fish, iguana, rock, etc.

        Once you've trained your model, you will give it sets of new input containing those features;
        it will return the predicted "label" (pet type) for that person.
    */

    test.take(5).foreach(x => println("features (Test Data) and Label ",x.features,x.label))

    train.take(5).foreach(x => println("features (Train Data) and Label ",x.features,x.label))

    predictionsAndLabels.filter(_._1 == 1).take(5).foreach(x => println("Potential Biker Buyer data (features) and predicted values (Label 1)",x._1,x._2))

    predictionsAndLabels.filter(_._2 == 1).take(5).foreach(x => println("Potential Biker Buyer data (features) and predicted values (Label 2)",x._1,x._2))

    val predictionsAndLabels2 = test.map {
      point => (dtree.predict(point.features), point.label,point)
    }

    predictionsAndLabels2.filter(_._1 == 1).take(5).foreach(x => println("Potential Biker Buyer data (features) and " +
      "predicted values (Label 1,2,3)",x._1,x._2,x._3))


    //save model
    //dtree.save(sc,"target/tmp/myDecisionTreeClassificationModel")
    //load model
    //val sameModel=DecisionTreeModel.load(sc,"target/tmp/myDecisionTreeClassificationModel")

    /*
    val predictionsAndLabels3 = test.map {
      point => (sameModel.predict(point.features), point.label,point)
    }
    predictionsAndLabels3.filter(_._1 == 1).take(5).foreach(x => println("Potential Biker Buyer data (features) and " +
      "predicted values (Label 1,2,3)",x._1,x._2,x._3))
    */

    spark.stop()

  }


}
