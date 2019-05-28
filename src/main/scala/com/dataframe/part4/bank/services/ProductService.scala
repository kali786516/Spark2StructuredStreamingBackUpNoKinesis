package com.dataframe.part4.bank.services

import java.util.UUID

import com.dataframe.part4.bank.entities._

/**
  * Created by kalit_000 on 5/24/19.
  */
trait ProductService extends ProductsDb {



  def addNewDepositProduct(name:String,minBalance:Int,ratePerYear:Double,
                          transactionAllowedPerMonth:Int=2):UUID = {

    val product = name match  {
      case "CoreChecking" => new CoreChecking(Dollars(minBalance),ratePerYear)
      case "StudentCheckings" => new StudentChecking(Dollars(minBalance),ratePerYear)
      case "RewardsSavings" => new RewardSavings(Dollars(minBalance),ratePerYear,transactionAllowedPerMonth)
    }

    saveDepositProduct(product)
    product.id
  }

  def addNewLendingProduct(annualFee:Double,apr:Double,rewardsPercent:Double):UUID = {
    val product = new CreditCard(annualFee,apr,rewardsPercent)
    saveLendingProduct(product)
    product.id
  }


}
