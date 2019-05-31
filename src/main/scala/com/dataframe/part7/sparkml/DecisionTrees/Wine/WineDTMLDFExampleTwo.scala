package com.dataframe.part7.sparkml.DecisionTrees.Wine

import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.classification.DecisionTreeClassifier
import org.apache.spark.sql.SparkSession
import org.apache.spark.mllib.linalg.VectorUDT
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types.StructType
import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator

/**
  * Created by kalit_000 on 5/29/19.
  */
object WineDTMLDFExampleTwo {

  def main(args: Array[String]): Unit = {

    val logger = Logger.getLogger("BikeBuyersDecisionTrees")
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

    val spark = SparkSession.builder().appName("Wine DT with DF Example Spark ML").master("local[*]").getOrCreate()
    val sc = spark.sparkContext

    val rawData = spark.read.format("csv").option("header","false").load("sparkMLDataSets/wine.data")

    val df = rawData.toDF("label","Alcohol","MalicAcid","Ash","ASkAlkalinity","Magneisum","TotalPhenols","Flavanoids","NonFlavanoidPhenols"
      ,"Proanthocyanins","ColorIntentisty","Hue","OD","Proline")


    val schema = new StructType()
      .add("features", new VectorUDT())

    val toDouble = udf[Double, String]( _.toDouble)

    //val df2 = df.withColumn("dependent_var", toDouble(df("dependent_var")))

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

    //val df2=df.withColumn("label_var", toDouble(df("label")))

    val assembler = new VectorAssembler().
    setInputCols(Array("Alcohol", "MalicAcid", "Ash","ASkAlkalinity","Magneisum","TotalPhenols","Flavanoids","NonFlavanoidPhenols"
      ,"Proanthocyanins","ColorIntentisty","Hue","OD","Proline"))
      .setOutputCol("features")

    val out = assembler.transform(df2)

    df2.printSchema()
    out.printSchema()

    out.registerTempTable("tempdf")

    val opdf=spark.sql("select label,features from tempdf")

    opdf.show(10)

    /*convert categorical vlaues to numerical*/
    val labelIndexer = new StringIndexer().setInputCol("label").setOutputCol("indexdLabel")

    val indexdData = labelIndexer.fit(opdf).transform(opdf)

    indexdData.selectExpr("label","indexdLabel","features").show(10)

    val Array(train,testdata) =indexdData.randomSplit(Array(.8, .2))

    val dtree = new DecisionTreeClassifier().setLabelCol("indexdLabel").setFeaturesCol("features").setMaxDepth(3).setImpurity("gini")

    val model = dtree.fit(train)

    /*evaluate how well out model performs*/
    val evaluator = new MulticlassClassificationEvaluator().setLabelCol("indexdLabel").setPredictionCol("prediction").setMetricName("f1")

    val transFormedData = model.transform(testdata)

    transFormedData.show(10)

    println(evaluator.setMetricName("accuracy").evaluate(transFormedData))

  }

}
