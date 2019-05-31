package com.dataframe.part7.sparkml.DecisionTrees.BikeBuyerADW.common

/**
  * Created by kalit_000 on 5/28/19.
  */

import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.Vector

trait LabeledPointConverter {
  def label():Double
  def features():Vector
  def toLabeledPoint() = LabeledPoint(label(),features())

}
