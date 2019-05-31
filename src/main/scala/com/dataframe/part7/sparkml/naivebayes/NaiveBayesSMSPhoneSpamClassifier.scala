package com.dataframe.part7.sparkml.naivebayes

/**
  * Created by kalit_000 on 5/28/19.
  */

import com.dataframe.part7.sparkml.DecisionTrees.BikeBuyerADW.common.PrintUtils.printMetrics
import com.dataframe.part7.sparkml.DecisionTrees.BikeBuyerADW.common.Stats
import com.dataframe.part7.sparkml.DecisionTrees.BikeBuyerADW.common.Stats.confusionMatrix
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.mllib.classification.NaiveBayes
import org.apache.spark.mllib.classification.NaiveBayesModel
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.feature.HashingTF
import org.apache.spark.mllib.feature.Word2Vec
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.rdd.RDD
//import examples.common.Application._
import org.apache.spark.mllib.feature.IDF
import org.apache.spark.sql.SparkSession
import org.apache.spark.util._

object NaiveBayesSMSPhoneSpamClassifier {

  def evaluateModel(model: NaiveBayesModel, test: RDD[LabeledPoint]) = {
    val predict = model.predict(test.map(_.features))

    test.take(5).foreach {
      x => println(s"Predicted: ${model.predict(x.features)}, Label: ${x.label}")
    }

    val predictionsAndLabels = test.map {
      point => (model.predict(point.features), point.label)
    }

    val stats = Stats(confusionMatrix(predictionsAndLabels))
    println(stats.toString)

    val metrics = new BinaryClassificationMetrics(predictionsAndLabels)
    printMetrics(metrics)
  }

  def tfidf(data: RDD[(String, Array[String])])(implicit sc: SparkContext) = {
    val docs = data.count.toDouble
    //TF - terms frequencies
    val tfs = data.map {
      t => (t._1, t._2.foldLeft(Map.empty[String, Int])((m, s) => m + (s -> (1 + m.getOrElse(s, 0)))))
    }

    //TF-IDF
    val idfs = data.flatMap(_._2).map((_, 1)).reduceByKey(_ + _).map {
      case (term, count) => (term, math.log(docs / (1 + count)))
    }.collectAsMap

    //idfs.lookup("").lift(0).getOrElse(0d)
    tfs.map {
      case (m, tf) =>
        (m, tf.map {
          case (term, freq) => (term, freq * idfs.getOrElse(term, 0d))
        })
    }
  }

  def main(args: Array[String]): Unit = {

    val logger = Logger.getLogger("BikeBuyersRForest")
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

    val spark = SparkSession.builder().appName("NaiveBayes exploiting TFIDF for spam classification").master("local[*]").getOrCreate()
    implicit val sc = spark.sparkContext

    val hash = new HashingTF(numFeatures = 100000)

    val raw = sc.textFile(args.headOption.getOrElse("sparkMLDataSets/") + "sms-labeled.txt").distinct().map {
      _.split("\\t+")
    }.map {
      a => (a(0), a(1).split("\\s+").map(_.toLowerCase()))
    }.map {
      t => (t._1, t._2, hash.transform(t._2))
    }.cache

    val idf = new IDF().fit(raw.map(_._3))
    val data = raw.map {
      t => LabeledPoint(if (t._1 == "spam") 1 else 0, idf.transform(t._3))
    }

    /*
      Briefly, feature is input; label is output.
      A feature is one column of the data in your input set. For instance,
      if you're trying to predict the type of pet someone will choose,
      your input features might include age, home region, family income, etc.
      The label is the final choice, such as dog, fish, iguana, rock, etc.

      Once you've trained your model, you will give it sets of new input containing those features;
      it will return the predicted "label" (pet type) for that person.
    */

    val Array(train, test) = data.randomSplit(Array(.8, .2), 102059L)
    val model = NaiveBayes.train(train)
    evaluateModel(model, test)

    //val termsInSpamMsgs = tfidf(raw.filter(_._1 == "spam").map(t => (t._1, t._2))).sortBy(_._2.values, ascending = false)
    //termsInSpamMsgs.take(10).foreach(println)

    test.map( p => (model.predict(p.features),p.label)).foreach(x => println("Naive Bayes data",x._1,x._2))

    spark.stop()


  }


}
