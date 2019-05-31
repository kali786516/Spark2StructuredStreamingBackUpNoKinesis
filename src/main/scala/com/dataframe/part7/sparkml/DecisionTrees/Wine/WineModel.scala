package com.dataframe.part7.sparkml.DecisionTrees.Wine

import com.dataframe.part7.sparkml.DecisionTrees.BikeBuyerADW.common.LabeledPointConverter
import com.dataframe.part7.sparkml.regression.LinearRegression.HouseModel
import org.apache.spark.mllib.linalg.Vectors

/**
  * Created by kalit_000 on 5/28/19.
  */

case class WineModel(label_data:Long,
                     alcohol:Double,
                     malic_acid:Double,
                     ash:Double,
                     magnesium:Double,
                     hue:Long,
                     color:Double,
                     intensity:Double,
                     alcalinity_of_ash:Double,
                     total_phenols:Double,
                     flavanoids:Double,
                     proanthocyanins:Double,
                     non_flavanoids:Double,
                     proline:Long
                    ) extends LabeledPointConverter {

  //Predict Price
  def label() = label_data
  // rest of the data
  def features() = WineModel.convert(this)
}


object WineModel {

  def apply(row: Array[String]) = new WineModel(
    row(0).toLong, row(1).toDouble,
    row(2).toDouble, row(3).toDouble,
    row(4).toDouble, row(5).toLong, row(6).toDouble,
    row(7).toDouble, row(8).toDouble, row(9).toDouble,
    row(10).toDouble, row(11).toDouble, row(12).toDouble,
    row(13).toLong
  )

  def convert(model: WineModel) = Vectors.dense(
    model.label_data.toLong,
    model.alcohol.toDouble,
    model.malic_acid.toDouble,
    model.ash.toDouble,
    model.magnesium.toDouble,
    model.hue.toLong,
    model.color.toDouble,
    model.intensity.toDouble,
    model.alcalinity_of_ash.toDouble,
    model.total_phenols.toDouble,
    model.flavanoids.toDouble,
    model.proanthocyanins.toDouble,
    model.non_flavanoids.toDouble,
    model.proline.toLong
  )

}
