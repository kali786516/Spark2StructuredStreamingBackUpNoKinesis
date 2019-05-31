package com.dataframe.part7.sparkml.regression.LinearRegression

/**
  * Created by kalit_000 on 5/28/19.
  */

import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.evaluation.RegressionMetrics
import org.apache.spark.mllib.feature.StandardScaler
import org.apache.spark.mllib.regression.{LabeledPoint, LinearRegressionWithSGD}
import org.apache.spark.mllib.stat.Statistics
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.util.control.Exception.catching

object HousePricesPrediction {

  def printColumnsStats(rdd: RDD[LabeledPoint]) = {
    val stats = Statistics.colStats(rdd.map { x => x.features })
    println(s"Max : ${stats.max}")
    println(s"Min : ${stats.min}")
    println(s"Mean : ${stats.mean}")
    println(s"Variance : ${stats.variance}")
  }

  def createLinearRegressionModel(rdd: RDD[LabeledPoint], numIterations: Int = 100, stepSize: Double = 0.01) = {
    LinearRegressionWithSGD.train(rdd, numIterations, stepSize)
  }

  def createDecisionTreeRegressionModel(rdd: RDD[LabeledPoint], maxDepth: Int = 10, maxBins: Int = 20) = {
    val impurity = "variance"
    DecisionTree.trainRegressor(rdd, Map[Int, Int](), impurity, maxDepth, maxBins)
  }

  def main(args: Array[String]): Unit = {

    val logger = Logger.getLogger("HousePricesPrediction")
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

    val spark = SparkSession.builder().appName("Regression for House prices predictions").master("local[*]").getOrCreate()
    val sc = spark.sparkContext

    val hdFile = sc.textFile(args.headOption.getOrElse("sparkMLDataSets/") + "house-data.csv")

    val houses = hdFile.map(_.split(",")).
      filter { t => catching(classOf[NumberFormatException]).opt(t(0).toLong).isDefined }.
      map { HouseModel(_).toLabeledPoint() }

    val scaler = new StandardScaler(withMean = true, withStd = true).fit(houses.map(dp => dp.features))

    val Array(train, test) = houses.
      map(dp => new LabeledPoint(dp.label, scaler.transform(dp.features))).
      randomSplit(Array(.9, .1), 10204L)

    val model = createDecisionTreeRegressionModel(train)

    val model2 = createLinearRegressionModel(train)

    /*
        Briefly, feature is input; label is output.
        A feature is one column of the data in your input set. For instance,
        if you're trying to predict the type of pet someone will choose,
        your input features might include age, home region, family income, etc.
        The label is the final choice, such as dog, fish, iguana, rock, etc.

        Once you've trained your model, you will give it sets of new input containing those features;
        it will return the predicted "label" (pet type) for that person.
    */

    test.take(5).foreach {
      x => println(s"Predicted Decision Tree Regression (Features): ${model.predict(x.features)}, Label: ${x.label}")
    }

    test.take(5).foreach {
      x => println(s"Predicted Linear Regression (Features): ${model2.predict(x.features)}, Label: ${x.label}")
    }

    val predictionsAndValues = test.map {
      point => (model.predict(point.features), point.label)
    }

    println("Mean house price: " + test.map { x => x.label }.mean())
    println("Max prediction error: " + predictionsAndValues.map { case (p, v) => math.abs(v - p) }.max)

    val metrics = new RegressionMetrics(predictionsAndValues)

    println(s"Mean Squared Error: ${metrics.meanSquaredError}")
    println(s"Root Mean Squared Error: ${metrics.rootMeanSquaredError}")
    println(s"Coefficient of Determination R-squared: ${metrics.r2}")
    println(s"Mean Absoloute Error: ${metrics.meanAbsoluteError}")
    println(s"Explained variance: ${metrics.explainedVariance}")

    val predictionsAndLabels2 = test.map {
      point => (model.predict(point.features), point.label,point)
    }

    predictionsAndLabels2.take(5).foreach(x => println("Potential House Price",x._1,x._2,x._3))

    spark.stop

  }
}
