package com.dataframe.part4.bank.services

import java.util.UUID
//import com.dataframe.part4.bank.entities.Product
import com.dataframe.part4.bank.entities._


trait ProductService extends ProductsDb {
  /**
    * // todo: (challenge?) how to disallow products of same name?
    *
    * @param name                        : name of the product
    * @param minBalance                  : the minimum balance required for the product
    * @param ratePerYear                 : the rate of interest earned by the end of the year
    * @param transactionsAllowedPerMonth : number of free transactions allowed for the product (optional)
    * @return the product id for the new product
    */
  def addNewDepositProduct(name: String, minBalance: Int, ratePerYear: Double,
                           transactionsAllowedPerMonth: Int = 2): UUID = {
    val product = name match {
      case "CoreChecking" => new CoreChecking(Dollars(minBalance), ratePerYear)
      case "StudentCheckings" => new StudentChecking(Dollars(minBalance), ratePerYear)
      case "RewardsSavings" => new RewardSavings(Dollars(minBalance), ratePerYear, transactionsAllowedPerMonth)
    }

    saveDepositProduct(product)
    product.id
  }

  def addNewLendingProduct(annualFee: Double, apr: Double, rewardsPercent: Double): UUID = {
    val product = new CreditCard(annualFee, apr, rewardsPercent)
    saveLendingProduct(product)
    product.id
  }
}
