package com.dataframe.part7.sparkml.DecisionTrees.BikeBuyerADW.common

/**
  * Created by kalit_000 on 5/28/19.
  */

import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics

object PrintUtils {
  def printMetrics(metrics: BinaryClassificationMetrics) = {
    // Precision by threshold
    val precision = metrics.precisionByThreshold
    precision.foreach {
      case (t, p) =>
        println(s"Threshold: $t, Precision: $p")
    }

    // Recall by threshold
    val recall = metrics.recallByThreshold
    recall.foreach {
      case (t, r) =>
        println(s"Threshold: $t, Recall: $r")
    }

    // F-measure
    val f1Score = metrics.fMeasureByThreshold
    f1Score.foreach {
      case (t, f) =>
        println(s"Threshold: $t, F-score: $f, Beta = 1")
    }

    val beta = 0.5
    val fScore = metrics.fMeasureByThreshold(beta)
    f1Score.foreach {
      case (t, f) =>
        println(s"Threshold: $t, F-score: $f, Beta = 0.5")
    }

    // AUPR
    val auPR = metrics.areaUnderPR
    println("Area under PR (precision-recall curve) = " + auPR)

    // AUROC
    val auROC = metrics.areaUnderROC
    println("Area under ROC (Receiver Operating Characteristic) = " + auROC)
  }
}
