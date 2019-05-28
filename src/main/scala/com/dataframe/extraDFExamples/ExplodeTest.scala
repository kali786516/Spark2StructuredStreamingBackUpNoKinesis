package com.dataframe.extraDFExamples

/**
  * Created by kalit_000 on 5/14/19.
  */

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

import org.apache.spark.sql.types._

object ExplodeTest {

  def main(args: Array[String]): Unit = {
    val logger = Logger.getLogger("HbIngestion")
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)
    val startTimeMillis = System.currentTimeMillis()

    val spark=SparkSession.builder()
      .appName("Test2").master("local").config("spark.driver.memory","2g").enableHiveSupport().getOrCreate()

    import spark.implicits._

    //val mySchema = StructType(StructField("keyValueMap",ArrayType(MapType(StringType,IntegerType))))

    /*
    val test = StructType(
      StructField(comments,ArrayType(StructType(StructField(comId,StringType,true),StructField(content,StringType,true)),true),true),
      StructField(createHour,StringType,true),
      StructField(gid,StringType,true),
      StructField(replies,ArrayType(StructType(StructField(content,StringType,true),StructField(repId,StringType,true)),true),true),
      StructField(revisions,ArrayType(StructType(StructField(modDate,StringType,true),StructField(revId,StringType,true)),true),true)
    )*/


    val jsonCompanies = List(
      """{"company":"NewCo","employees":[{"firstName":"Justin","LastName":"Pihony"},{"firstName":"Jane","LastName":"Doe"}]}""",
      """{"company":"FamilyCo","employees":[{"firstName":"Rigel","LastName":"Pihony"},{"firstName":"Rory","LastName":"Pihony"}]}""",
      """{"company":"OldCo","employees":[{"firstName":"Mary","LastName":"Louise"},{"firstName":"Joe","LastName":"Bob"}]}"""
    )

    val companiesRdd = spark.sparkContext.makeRDD(jsonCompanies)

    val companiesDF = spark.read.json(companiesRdd)

    companiesDF.show(false)

    companiesDF.printSchema()

    println(companiesDF.schema)

    //println(companiesDF.schema.fields.mkString(",\n"))

    //val testScheam=StructType(StructField("company",StringType,true), StructField("employees",ArrayType(StructType(StructField("LastName",StringType,true), StructField("firstName",StringType,true)),true),true))

    //spark.read.schema(testScheam).json(companiesRdd).show(10)

    val comapaniesDFTemp=companiesDF.select($"company",explode($"employees").as("employees"))

    comapaniesDFTemp.show(10)

    val employeeDF=comapaniesDFTemp.select($"company",expr("employees.firstName as firstName"))

    employeeDF.select($"*",when($"company" === "FamilyCo","Premium").when($"company" === "OldCo","Legacy").otherwise("Standard")).show(10)

    companiesDF.select($"company",posexplode($"employees").as(Seq("employeesPosition","employee"))).show(10)

  }

}
