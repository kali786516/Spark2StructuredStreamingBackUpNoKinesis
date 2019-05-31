package com.dataframe.part7.sparkml.regression.LinearRegression

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, regexp_replace}
import org.apache.spark.sql.types.{StructType, _}
import com.google.common.collect.ImmutableMap
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.{OneHotEncoder, OneHotEncoderEstimator, StringIndexer, VectorAssembler}
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.ml.evaluation.RegressionEvaluator
import vegas._
import vegas.render.WindowRenderer._
import vegas.sparkExt._

/**
  * Created by kalit_000 on 5/29/19.
  */


object AutomobileRegression {

  def main(args: Array[String]): Unit = {

    val logger = Logger.getLogger("BikeBuyersDecisionTrees")
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

    val spark = SparkSession.builder().appName("Predicting the price of an automobile given a set of features").master("local[*]").getOrCreate()
    val sc = spark.sparkContext

    val rawData=spark.read.format("csv")
      .option("header","true")
      .load("sparkMLDataSets/imports-85.data")

    /*
    import spark.implicits._

    val schemaUntyped = StructType(
        StructField("symboling", StringType,true) ::
        StructField("normalizedlosses", StringType,true) ::
        StructField("make", StringType,true) ::
        StructField("fueltype", StringType,true) ::
        StructField("numofdoors", StringType,true) ::
          StructField("bodystyle", StringType,true) ::
          StructField("drivewheels", StringType,true) ::
          StructField("enginelocation", StringType,true) ::
          StructField("wheelbase", StringType,true) ::
          StructField("length", StringType,true) ::
          StructField("width", StringType,true) ::
          StructField("height", StringType,true) ::
          StructField("curbweight", StringType,true) ::
          StructField("enginetype", StringType,true) ::
          StructField("numofcylinders", StringType,true) ::
          StructField("enginesize", StringType,true) ::
          StructField("fuelsystem", StringType,true) ::
          StructField("bore", StringType,true) ::
          StructField("stroke", StringType,true) ::
          StructField("compressionratio", StringType,true) ::
          StructField("horsepower", StringType,true) ::
          StructField("peakrpm", StringType,true) ::
          StructField("highwaympg", StringType,true) ::
          StructField("price", StringType,true) ::
          Nil)

    val rawData = spark.read.format("csv")
      .option("header","true")
      .schema(schemaUntyped)
      .load("sparkMLDataSets/imports-85.data")*/

    println("-" * 100)

    println("Sample Data 10 Records")
    rawData.show(10)

    println(rawData.count())

    println("-" * 100)

    println("cast data sets")

    rawData.createOrReplaceTempView("rawDataTable")

    val dataSet = spark.sql("select cast(price as double) as price ,make,numofdoors,bodystyle ,drivewheels," +
      "wheelbase,curbweight," +
      "numofcylinders,enginesize," +
      "cast(horsepower as float) as horsepower," +
      "cast(peakrpm as float) as peakrpm from rawDataTable")

    val cleanedDataSet=dataSet.na.replace("*", ImmutableMap.of("?", "null")).na.drop()

    cleanedDataSet.show(10)

    println(cleanedDataSet.count())

    val Array(traindata,testdata) = cleanedDataSet.randomSplit(Array(.8, .2))


    println("String Indexer Step 1 ....................................................")

    val stringIndexColumns = Seq("make","numofdoors","bodystyle",
      "drivewheels","wheelbase","numofcylinders","enginesize")

    val stringIndexer = stringIndexColumns.map { colName =>
      new StringIndexer().setInputCol(colName).setOutputCol(colName + "_indexed").fit(cleanedDataSet)
    }

    val pipeline = new Pipeline().setStages(stringIndexer.toArray)

    val StringIndexedDF = pipeline.fit(traindata).transform(traindata)

    print("-" * 100)

    print("Data Set After StringIndexer addinging _indexex")

    StringIndexedDF.show(10)

    println("Ecoder Step 2 ....................................................")

    //val encoderColumns = Seq("\"make\",\"numofdoors\",\"bodystyle\",\n      \"drivewheels\",\"wheelbase\",\"numofcylinders\",\"enginesize\")

    val make_encoder = new OneHotEncoder()
      .setInputCol("make_indexed")
      .setOutputCol("make_encoded")

    val WorkClass_df = make_encoder.transform(StringIndexedDF)

    val numofdoors_encoder = new OneHotEncoder()
      .setInputCol("numofdoors_indexed")
      .setOutputCol("numofdoors_encoded")

    val numofdoors_df = numofdoors_encoder.transform(WorkClass_df)

    val bodystyle_encoder = new OneHotEncoder()
      .setInputCol("bodystyle_indexed")
      .setOutputCol("bodystyle_encoded")

    val bodystyle_df = bodystyle_encoder.transform(numofdoors_df)

    val drivewheels_encoder = new OneHotEncoder()
      .setInputCol("drivewheels_indexed")
      .setOutputCol("drivewheels_encoded")

    val drivewheels_df = drivewheels_encoder.transform(bodystyle_df)

    val wheelbase_encoder = new OneHotEncoder()
      .setInputCol("wheelbase_indexed")
      .setOutputCol("wheelbase_encoded")

    val wheelbase_df = wheelbase_encoder.transform(drivewheels_df)

    val numofcylinders_encoder = new OneHotEncoder()
      .setInputCol("numofcylinders_indexed")
      .setOutputCol("numofcylinders_encoded")

    val numofcylinders_df = numofcylinders_encoder.transform(wheelbase_df)

    val enginesize_encoder = new OneHotEncoder()
      .setInputCol("enginesize_indexed")
      .setOutputCol("enginesize_encoded")

    val enginesize_df = enginesize_encoder.transform(numofcylinders_df)

    val dataSetAfterStringIndexerAndEconder = enginesize_df

    println("-" * 100)

    println("Data Set After Encoder adding _encoded")

    dataSetAfterStringIndexerAndEconder.show(10)

    println("-" * 100)

    println("Vector Assembler Step  3....................................................")

    val assembler = new VectorAssembler()
      .setInputCols(Array("make_encoded","numofdoors_encoded","bodystyle_encoded","drivewheels_encoded", "wheelbase_encoded",
        "numofcylinders_encoded","enginesize_encoded"))
      .setOutputCol("features")

    val assemberDF = assembler.transform(dataSetAfterStringIndexerAndEconder)

    assemberDF.show(10)

    println("Linear Regression Step  4....................................................")

    //maxIte is number of epochs for which we run the training process
    //
    val lr = new LinearRegression().setMaxIter(100).setRegParam(1.0).setElasticNetParam(0.8).setLabelCol("price").setFeaturesCol("features")

    val linearRegModel = lr.fit(assemberDF)

    //println(linearRegModel.coefficients)

    val withLinearFit = linearRegModel.transform(assemberDF)

    println("Linear Regression Step  5 OP DATA....................................................")

    withLinearFit.show(10)

    val evaluator = new RegressionEvaluator().setLabelCol("price").setPredictionCol("prediction").setMetricName("r2")

    val r2 = evaluator.evaluate(withLinearFit)

    println("R2 Score " +r2)

    // lets create a graph using vegas

    val plotDF = withLinearFit.selectExpr("price","prediction")

    val plot = Vegas("Linear Regression").
               withDataFrame(plotDF).
               mark(Bar).
               encodeX("prediction").
               encodeY("price")

    //plot.show



    /*
    val plot = Vegas("Country Pop").
      withData(
        Seq(
          Map("country" -> "USA", "population" -> 314),
          Map("country" -> "UK", "population" -> 64),
          Map("country" -> "DK", "population" -> 80)
        )
      ).
      encodeX("country", Nom).
      encodeY("population", Quant).
      mark(Bar)

    plot.show*/


   // val dataset2 = rawData.withColumn("price",regexp_replace(col("price"),"\\?","None"))
      //.withColumn("Occupation",regexp_replace(col("Occupation"),"\\?","None"))

    //dataset2.filter($"age" === "35" || $"Education" === "HS-grad").selectExpr("WorkClass","Occupation").show(10)

    //val dataset3 = dataset2.filter($"WorkClass" =!= "None" || $"Occupation" =!= "None")


    /*val dataSet = rawData.selectExpr("cast(price as float) as price","make","'num-of-doors'",
                        "'body-style'","'drive-wheels'","cast('wheel-base' as double) as wheelbase",
        "cast('curb-weight' as float) as curbweight","'num-of-cylinders'",
        "cast('engine-size' as float) as enginesize","cast(horsepower as float) as horsepower","cast('peak-rpm' as float) as peakrpm")

   dataSet.show(10)*/



   sc.stop()


  }

}
