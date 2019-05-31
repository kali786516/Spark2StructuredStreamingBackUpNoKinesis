package com.dataframe.part7.sparkml.randomforest

import com.dataframe.part7.sparkml.DecisionTrees.BikeBuyerADW.BikeBuyerModel
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import com.google.common.collect.ImmutableMap
import org.apache.spark.ml.feature.{OneHotEncoder, OneHotEncoderEstimator, StringIndexer, VectorAssembler}
import org.apache.spark.ml.{Pipeline, PipelineStage}
import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator

import scala.collection.mutable

/**
  * Created by kalit_000 on 5/29/19.
  */
object AdultSensesRandomForest {

  def main(args: Array[String]): Unit = {

    val logger = Logger.getLogger("BikeBuyersDecisionTrees")
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

    val spark = SparkSession.builder().appName("Predicting Whether a person's income is greater than 50K").master("local[*]").getOrCreate()
    val sc = spark.sparkContext

    val rawData = spark.read.format("csv")
                       .option("header","false")
                       .option("ignoreLeadingWhiteSpace","true").load("sparkMLDataSets/adult.csv")

    val dataSet = rawData.toDF("Age","WorkClass","FnlWgt","Education","EducationNum",
                               "MaritalStatus","Occupation","Relationship","Race","Gender","CapitalGain",
                               "CapitalLoss","HoursPerWeek","NativeCountry","Label")

    import spark.implicits._

    dataSet.filter($"age" === "35" || $"Education" === "HS-grad").selectExpr("WorkClass","Occupation").show(10)

    val dataset2 = dataSet.withColumn("WorkClass",regexp_replace(col("WorkClass"),"\\?","None"))
                          .withColumn("Occupation",regexp_replace(col("Occupation"),"\\?","None"))

    dataset2.filter($"age" === "35" || $"Education" === "HS-grad").selectExpr("WorkClass","Occupation").show(10)

    val dataset3 = dataset2.filter($"WorkClass" =!= "None" || $"Occupation" =!= "None")

    val dataset4 = dataset3.na.drop(how="any")

    dataset4.filter($"age" === "35" || $"Education" === "HS-grad").selectExpr("WorkClass","Occupation").show(10)

    dataset4.selectExpr("WorkClass").distinct().show(10)

    val dataset5 = dataset4.selectExpr("cast(Age as long) as Age","WorkClass","FnlWgt","Education","cast(EducationNum as long) as EducationNum",
      "MaritalStatus","Occupation","Relationship","Race","Gender","cast(CapitalGain as long) as CapitalGain",
      "cast(CapitalLoss as long) as CapitalLoss","cast(HoursPerWeek as long) as HoursPerWeek","NativeCountry","Label")

    val Array(traindata,testdata) = dataset5.randomSplit(Array(.8, .2))

    val stringIndexColumns = Seq("WorkClass","Education","MaritalStatus",
      "Occupation","Relationship","Race","Gender","NativeCountry")

    val stringIndexer = stringIndexColumns.map { colName =>
      new StringIndexer().setInputCol(colName).setOutputCol(colName + "_indexed").fit(dataset5)
    }

    print("-" * 100)

    print("Data Set After ETL")

    dataset5.show(10)

    print("-" * 100)

    val pipeline = new Pipeline().setStages(stringIndexer.toArray)

    val transformedDF2 = pipeline.fit(traindata).transform(traindata)

    print("-" * 100)

    print("Data Set After StringIndexer addinging _indexex")

    transformedDF2.show(10)

    print("-" * 100)


    val encoderColumns = Seq("WorkClass_encoded","Education_encoded","MaritalStatus_encoded",
      "Occupation_encoded","Relationship_encoded","Race_encoded","Gender_encoded","NativeCountry_encoded")

    val WorkClass_encoder = new OneHotEncoder()
      .setInputCol("WorkClass_indexed")
      .setOutputCol("WorkClass_encoded")

     val WorkClass_df = WorkClass_encoder.transform(transformedDF2)

    val Education_encoder = new OneHotEncoder()
      .setInputCol("Education_indexed")
      .setOutputCol("Education_encoded")

    val Education_df = Education_encoder.transform(WorkClass_df)

    val MaritalStatus_encoder = new OneHotEncoder()
      .setInputCol("MaritalStatus_indexed")
      .setOutputCol("MaritalStatus_encoded")

    val MaritalStatus_df = MaritalStatus_encoder.transform(Education_df)

    val Occupation_encoder = new OneHotEncoder()
      .setInputCol("Occupation_indexed")
      .setOutputCol("Occupation_encoded")

    val Occupation_df = Occupation_encoder.transform(MaritalStatus_df)

    val Relationship_encoder = new OneHotEncoder()
      .setInputCol("Relationship_indexed")
      .setOutputCol("Relationship_encoded")

    val Relationship_df = Relationship_encoder.transform(Occupation_df)

    val Race_encoder = new OneHotEncoder()
      .setInputCol("Race_indexed")
      .setOutputCol("Race_encoded")

    val Race_df = Race_encoder.transform(Relationship_df)

    val Gender_encoder = new OneHotEncoder()
      .setInputCol("Gender_indexed")
      .setOutputCol("Gender_encoded")

    val Gender_df = Gender_encoder.transform(Race_df)

    val NativeCountry_encoder = new OneHotEncoder()
      .setInputCol("NativeCountry_indexed")
      .setOutputCol("NativeCountry_encoded")

    val NativeCountry_df = NativeCountry_encoder.transform(Gender_df)

    println("-" * 100)

    println("Data Set After Encoder adding _encoded")

    NativeCountry_df.show(10)

    println("-" * 100)

    val labelIndexer = new StringIndexer().setInputCol("Label").setOutputCol("Label_index")

    val labelIndexerDF = labelIndexer.fit(NativeCountry_df).transform(NativeCountry_df)

    println("-" * 100)

    println("Data Set After labelIndexing")

    labelIndexerDF.show(10)

    println("-" * 100)

    val finalDF = labelIndexerDF.selectExpr("Age","EducationNum","CapitalGain","HoursPerWeek",
                  "WorkClass_encoded","Education_encoded","MaritalStatus_encoded",
                   "Occupation_encoded","Relationship_encoded","Race_encoded","Gender_encoded",
                   "NativeCountry_encoded","Label_index")

    println("-" * 100)

    println("Columns What we Required for ML Modelling")

    finalDF.show(10)

    println("-" * 100)

    val assembler = new VectorAssembler()
      .setInputCols(Array("Age","EducationNum","CapitalGain","HoursPerWeek", "WorkClass_encoded","Education_encoded","MaritalStatus_encoded",
      "Occupation_encoded","Relationship_encoded","Race_encoded","Gender_encoded", "NativeCountry_encoded"))
      .setOutputCol("features")

    val output = assembler.transform(finalDF)

    println("-" * 100)

    println("After adding features to the data sets")

    output.show(10)

    println("-" * 100)

    val rf = new RandomForestClassifier().setLabelCol("Label_index").setFeaturesCol("features").setMaxDepth(30)

    val labelIndexer2 = new StringIndexer().setInputCol("Label_index").setOutputCol("Label_index_2")

    val model = rf.fit(output)

    println(model.toDebugString)

    val predictions = model.transform(output)

    println("-" * 100)

    println("Predicted Data Set")

    predictions.toDF().show(10)

    println("-" * 100)

    val evaluator = new MulticlassClassificationEvaluator().setLabelCol("Label_index").setPredictionCol("prediction").setMetricName("accuracy")

    println("Accuracy " + evaluator.evaluate(predictions))

    println("-" * 100)

    println("Bad Predicted Data Set")

    // predictions which are wrong
    predictions.toDF().filter($"Label_index" =!= $"prediction").show(10)

    println("-" * 100)

    println(predictions.toDF().filter($"Label_index" =!= $"prediction").count())

    println(predictions.toDF().filter($"Label_index" === $"prediction").count())

    //dataSet.drop("FnlWgt").show(10)

    //val cleanedDataSet=dataSet.na.replace("*", ImmutableMap.of("?", "null")).na.drop()

    //cleanedDataSet.show(10)

    //val dataset3 = dataset2.na.replace("*", ImmutableMap.of("None", "unnamed"))

    //dataSet.na.replace("WorkClass", ImmutableMap.of("?", "None"))
    //dataSet.na.replace("*", ImmutableMap.of("?", "None"))
    //val predictions = model.transform(testdata)

    //val pipeline5 = new Pipeline().setStages(Array(labelIndexer2,rf))

    //val model = pipeline5.fit(output)


    /*
    val pipeline3 = new Pipeline()

    val  transformedDF3 = pipeline3.fit(labelIndexerDF).transform(labelIndexerDF)

    transformedDF3.show(10)
    */


    //val pipeline = new Pipeline().setStages(Array(stringIndexer))

/*
val encoders = new OneHotEncoderEstimator().setInputCols(Array("WorkClass_indexed","Education_indexed","MaritalStatus_indexed",
  "Occupation_indexed","Relationship_indexed","Race_indexed","Gender_indexed","NativeCountry_indexed"))
  .setOutputCols(Array("WorkClass_encoded","Education_encoded","MaritalStatus_encoded",
    "Occupation_encoded","Relationship_encoded","Race_encoded","Gender_encoded","NativeCountry_encoded"))
*/




//val pipeline = new Pipeline().setStages(Array(stringIndexer))

//val transformedDF2 = pipeline.fit(traindata).transform(traindata)

//transformedDF2.show(10)

//transformedDF2.drop("WorkClass_indexed","Education_indexed","MaritalStatus_indexed",
 // "Occupation_indexed","Relationship_indexed","Race_indexed","Gender_indexed","NativeCountry_indexed")




//val encodersdata = encoders.fit(transformedDF2)


//val transformedDF3 = pipeline.fit(transformedDF2).transform(transformedDF2)

//transformedDF3.show(10)

/*
/*convert categorical vlaues to numerical*/
val WorkClassIndexer = new StringIndexer()
                       .setInputCol("WorkClass").setOutputCol("WorkClass_index")

val EducationIndexer = new StringIndexer()
                      .setInputCol("Education").setOutputCol("Education_index")

val MaritalStatusIndexer = new StringIndexer()
                           .setInputCol("MaritalStatus").setOutputCol("MaritalStatus_index")

val OccupationIndexer = new StringIndexer()
                           .setInputCol("Occupation").setOutputCol("Occupation_index")

val RelationshipIndexer = new StringIndexer()
                        .setInputCol("Relationship").setOutputCol("Relationship_index")

val RaceIndexer = new StringIndexer()
                         .setInputCol("Race").setOutputCol("Race_index")

val GenderIndexer = new StringIndexer()
                    .setInputCol("Gender").setOutputCol("Gender_index")

val NativeCountryIndexer = new StringIndexer()
                           .setInputCol("NativeCountry").setOutputCol("NativeCountry_index")

val WorkClassEncoder = new OneHotEncoder()
  .setInputCol("WorkClass_index").setOutputCol("WorkClass_encoded")

val EducationEncoder = new OneHotEncoder()
  .setInputCol("Education_index").setOutputCol("Education_encoded")

val MaritalStatusEncoder = new OneHotEncoder()
  .setInputCol("MaritalStatus_index").setOutputCol("MaritalStatus_encoded")

val OccupationEncoder = new OneHotEncoder()
  .setInputCol("Occupation_index").setOutputCol("Occupation_encoded")

val RelationshipEncoder = new OneHotEncoder()
  .setInputCol("Relationship_index").setOutputCol("Relationship_encoded")

val RaceEncoder = new OneHotEncoder()
  .setInputCol("Race_index").setOutputCol("Race_encoded")

val GenderEncoder = new OneHotEncoder()
  .setInputCol("Gender_index").setOutputCol("Gender_encoded")

val NativeCountryEncoder = new OneHotEncoder()
  .setInputCol("NativeCountry_index").setOutputCol("NativeCountry_encoded")


val labelIndexer = new StringIndexer().setInputCol("Label").setOutputCol("Label_index")
/*
//val indexdDF = labelIndexer.fit(dataset5).transform(dataset5)



val IndexedColumns = Seq("WorkClass_indexed","Education_indexed","MaritalStatus_indexed",
                          "Occupation_indexed","Relationship_indexed","Race_indexed","Gender_indexed","NativeCountry_indexed")

val encoders = IndexedColumns.map { colName =>
 new OneHotEncoder().setInputCol(colName).setOutputCol(colName + "_encoded")
}*/

//val stages = new mutable.ArrayBuffer[PipelineStage]()

//stages += indexers
//stages += encoders
//stages += labelIndexer

//val pipeline = new Pipeline().setStages((stages.toArray))

//val stages2 = Array(indexers,encoders,labelIndexer)

*/




/*

val pipeline1 = new Pipeline().setStages(Array(WorkClassIndexer,EducationIndexer,MaritalStatusIndexer,OccupationIndexer,
                                               RelationshipIndexer,RaceIndexer,GenderIndexer,NativeCountryIndexer))


val pipeline2 = new Pipeline().setStages(Array(WorkClassEncoder,EducationEncoder,MaritalStatusEncoder,OccupationEncoder,
                                               RelationshipEncoder,RaceEncoder,GenderEncoder,NativeCountryEncoder))

val pipeline3 = new Pipeline().setStages(Array(labelIndexer))

val pipeline4 = new Pipeline().setStages(Array(pipeline1,pipeline2,pipeline3))

/*val pipeline2 = pipeline1.setStages(Array(WorkClassEncoder,EducationEncoder,MaritalStatusEncoder,OccupationEncoder,
                                               RelationshipEncoder,RaceEncoder,GenderEncoder,NativeCountryEncoder))*/

val transformedDF1 = pipeline4.fit(traindata).transform(traindata)

//val transformedDF2 = pipeline2.fit(traindata).transform(traindata)

transformedDF1.show(10,false)
//transformedDF2.show(10,false)

*/


/*
val assembler = new VectorAssembler()
  .setInputCols(Array("gender", "age", "weight", "height", "indexedJob"))
  .setOutputCol("features")

val pipeline = new Pipeline()
  .setStages(Array(assembler, standardScaler("features"), lr))

  https://www.programcreek.com/scala/org.apache.spark.ml.Pipeline
  */

sc.stop()







}

}
