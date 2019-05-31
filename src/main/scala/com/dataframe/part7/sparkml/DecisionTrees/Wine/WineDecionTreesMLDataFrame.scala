package com.dataframe.part7.sparkml.DecisionTrees.Wine

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.linalg.VectorUDT
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.ml.classification.DecisionTreeClassifier
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.functions._
import org.apache.spark.mllib.linalg.{Vectors => OldVectors}
import org.apache.spark.ml.linalg.{Vectors => NewVectors}
/**
  * Created by kalit_000 on 5/28/19.
  */

object WineDecionTreesMLDataFrame {

  def main(args: Array[String]): Unit = {

    val logger = Logger.getLogger("BikeBuyersDecisionTrees")
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

    val spark = SparkSession.builder().appName("Wine DT with DF Example Spark ML").master("local[*]").getOrCreate()
    val sc = spark.sparkContext

    val rawData = spark.read.format("csv").option("header","false").load("sparkMLDataSets/wine.data")

    val df = rawData.toDF("label","Alcohol","MalicAcid","Ash","ASkAlkalinity","Magneisum","TotalPhenols","Flavanoids","NonFlavanoidPhenols"
                          ,"Proanthocyanins","ColorIntentisty","Hue","OD","Proline")

    //https://www.programcreek.com/scala/org.apache.spark.mllib.linalg.Vectors

    import spark.implicits._

    val DF3 = sc.textFile("sparkMLDataSets/wine.data").filter(_.nonEmpty)
                .map(_.split(",").map(_.toDouble)).map(x => (x(0),
                Vectors.dense(x(1),x(2),x(3),x(4),x(5),x(6),x(7),x(8),x(9),x(10),x(11),x(12),x(13)))).toDF("label","features")

    DF3.show(10)

    DF3.take(5).foreach(println)

    //val parsedData = sc.textFile("sparkMLDataSets/wine.data").filter(_.nonEmpty).map(x => Vectors.dense(x.split(',').map(_.toDouble))).cache()


    val assembler = new VectorAssembler()
      .setInputCols(Array("Alcohol", "MalicAcid", "Ash","ASkAlkalinity","Magneisum","TotalPhenols","Flavanoids","NonFlavanoidPhenols"
        ,"Proanthocyanins","ColorIntentisty","Hue","OD","Proline"))
      .setOutputCol("features")

    df.registerTempTable("test")

    val df2=spark.sql("select cast(label as int) as label,cast(Alcohol as int) as Alcohol,cast(MalicAcid as double) as MalicAcid," +
      "cast(Ash as double) as Ash," +
      "cast(ASkAlkalinity as double) as ASkAlkalinity," +
      "cast(Magneisum as double) as Magneisum," +
      "cast(TotalPhenols as double) as TotalPhenols," +
      "cast(Flavanoids as double) as Flavanoids," +
      "cast(NonFlavanoidPhenols as double) as NonFlavanoidPhenols," +
      "cast(Proanthocyanins as double) as Proanthocyanins," +
      "cast(ColorIntentisty as double) as ColorIntentisty," +
      "cast(Hue as double) as Hue," +
      "cast(OD as double) as OD," +
      "cast(Proline as double) as Proline from test")


   /* val df2 = df.selectExpr("cast(Alcohol as int) as Alcohol",
      "cast(MalicAcid as double) as MalicAcid",
      "cast(Ash as double) as Ash",
      "cast(ASkAlkalinity as double) as ASkAlkalinity",
      "cast(Magneisum as double) as Magneisum",
      "cast(TotalPhenols as double) as TotalPhenols",
      "cast(Flavanoids as double) as Flavanoids",
      "cast(NonFlavanoidPhenols as double) as NonFlavanoidPhenols",
      "cast(Proanthocyanins as double) as Proanthocyanins",
      "cast(ColorIntentisty as double) as ColorIntentisty",
      "cast(Hue as double) as Hue",
      "cast(OD as double) as OD",
      "cast(Proline as double) as Proline")*/

    df2.show(10)

    val output = assembler.transform(df2)

    output.show(10)

    val vectorizedData = DF3.selectExpr("label","features")

    vectorizedData.show(10)

    vectorizedData.take(5).foreach(println)

    /*change categorical values to numerical value*/

    /*
        +-----+--------------------+-----------+
        |label|            features|indexdLabel|
        +-----+--------------------+-----------+
        |    1|[14.0,1.71,2.43,1...|        1.0|
        |    1|[13.0,1.78,2.14,1...|        1.0|
        |    1|[13.0,2.36,2.67,1...|        1.0|
        |    1|[14.0,1.95,2.5,16...|        1.0|
        |    1|[13.0,2.59,2.87,2...|        1.0|
        |    1|[14.0,1.76,2.45,1...|        1.0|
        |    1|[14.0,1.87,2.45,1...|        1.0|
        |    1|[14.0,2.15,2.61,1...|        1.0|
        |    1|[14.0,1.64,2.17,1...|        1.0|
        |    1|[13.0,1.35,2.27,1...|        1.0|
        +-----+--------------------+-----------+
    */

    val labelIndexer = new StringIndexer().setInputCol("label").setOutputCol("indexdLabel")

    val indexdData = labelIndexer.fit(vectorizedData).transform(vectorizedData)

    import spark.implicits._

    spark.udf.register("to_vector",(label:Double) => Vectors.dense(label))

    val finaldata = indexdData.withColumn("vector_label",callUDF("to_vector",$"indexdLabel"))

    finaldata.show(10)

    val Array(train,testdata) =finaldata.randomSplit(Array(.8, .2))

    //to_vector = udf(lambda a: Vectors.dense(a), VectorUDT())

    //data = df.select("label", to_vector("array").alias("features"))

    //spark.udf.register("to_vector",(fullString:String,splitter:String) => fullString.split(splitter).map(_.capitalize).mkString(splitter))


    val dtree = new DecisionTreeClassifier().setLabelCol("indexdLabel").setFeaturesCol("vector_label").setMaxDepth(3).setImpurity("gini")

    val model = dtree.fit(train)

    /*
    /*evaluate how well out model performs*/
    val evaluator = new MulticlassClassificationEvaluator().setLabelCol("indexdLabel").setPredictionCol("prediction").setMetricName("f1")

    val transformedData = model.transform(testdata)
    transformedData.show(5)

    evaluator.evaluate(transformedData)*/

    //println("Accuracy :-"+evaluator.getMetricName())


    sc.stop()



  }

}
