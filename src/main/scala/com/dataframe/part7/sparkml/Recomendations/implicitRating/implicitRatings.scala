package com.dataframe.part7.sparkml.Recomendations.implicitRating

import com.google.common.collect.ImmutableMap
import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.recommendation.ALS
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
  * Created by kalit_000 on 5/30/19.
  */
object implicitRatings {

  def main(args: Array[String]): Unit = {

    val logger = Logger.getLogger("BikeBuyersDecisionTrees")
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

    val spark = SparkSession.builder().appName("Use Collaborative Filtering for movie Recommendations").master("local[*]").getOrCreate()
    val sc = spark.sparkContext

    import spark.implicits._

    val rawData=spark.read.format("csv")
      .option("header","true")
      .option("delimiter","\t")
      .load("sparkMLDataSets/lastfm/user_artists.dat")

    println("-" * 100)

    println("Sample Data 10 Records")
    rawData.show(10)

    println(rawData.count())

    println("-" * 100)

    println("cast data sets")

    rawData.createOrReplaceTempView("rawDataTable")

    val dataSet = spark.sql("select cast(userID as int) as userID ,cast(artistID as int) as artistID," +
      "cast(weight as int) as weight from rawDataTable")

    val cleanedDataSet=dataSet.na.replace("*", ImmutableMap.of("?", "null")).na.drop()

    cleanedDataSet.show(10)

    println(cleanedDataSet.count())

    println("-" * 100)

    println("Lets Describe the weight columns")

    cleanedDataSet.describe("weight").show(100)

    /* standardization not sure what it is Z = x - u / o Z = scaled value u = mean and o = standard deviation */
    val df = cleanedDataSet.select(
      mean($"weight").alias("mean_weight"),
      stddev($"weight").alias("stddev_weight")).crossJoin(cleanedDataSet).withColumn("weight_Scaled",(col("weight") - col("weight")) / col("stddev_weight"))

    df.show(10,false)

    println("Split the Data Step  1....................................................")

    val Array(traindata,testdata) = df.randomSplit(Array(.8, .2))

    println("Build ALS Step  2....................................................")

    //cold strategy if the algorithm encounters new productid or userid while buildint the model it will just drop the record (streaming data)
    // setImplicitPrefs(true) --> the value should be true for Implicit data sets
    //val als = new ALS().setMaxIter(10).setRegParam(0.2).setUserCol("userID").setItemCol("artistID").setImplicitPrefs(true)
      //.setRatingCol("weight_Scaled").setColdStartStrategy("drop")

    val als = new ALS().setMaxIter(10).setRegParam(0.2).setUserCol("userID").setItemCol("artistID").setImplicitPrefs(false)
                       .setRatingCol("weight_Scaled").setColdStartStrategy("drop")

    val model = als.fit(traindata)

    println("Get Predictions Step  3....................................................")

    val predictions = model.transform(testdata)

    predictions.show(10)

    predictions.describe("weight_scaled","prediction").show(100)


    println("Join artists Step  4....................................................")


    val artistData=spark.read.format("csv")
      .option("header","true")
      .option("delimiter","\t")
      .load("sparkMLDataSets/lastfm/artists.dat")

    artistData.show(10,false)

    val usersDF = artistData.selectExpr("id as userID")

    /*
    val userRecs = model.recommendForUserSubset(usersDF,3)

    userRecs.show(10,false)


    println("Lets get recommendations for singe user and split the data after exploding and print the results Step  8....................................................")

    userRecs.createOrReplaceTempView("userRecs")


    val moveisDFforOneUSer148 = spark.sql("select userrecs.recommendations from userRecs where userrecs.userID = '471' ")

    moveisDFforOneUSer148.show(10,false)

    userRecs.registerTempTable("tobeexploded")

    val testDf1 = spark.sql("select explode(recommendations) as recommendations from tobeexploded")

    testDf1.createOrReplaceTempView("test2")

    val finalDF = spark.sql("select trim(split(cast(recommendations as string),',')[0]) as artist_id ,trim(split(cast(recommendations as string),',')[1]) as artist_weight from test2")
      .select(translate($"artist_id","[","").alias("id"),translate($"artist_weight","]","").alias("artist_weight_new"))

    finalDF.show(10)

    val recommnendedMovies = artistData.join(finalDF,"id").orderBy($"artist_weight_new".asc).selectExpr("name","url","artist_weight_new as rating")

    artistData.join(finalDF,"id").orderBy($"artist_weight_new".asc).selectExpr("name","url","artist_weight_new as rating").show(10,false)
    */

    sc.stop()

  }

}
