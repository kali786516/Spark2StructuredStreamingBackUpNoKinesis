package com.dataframe.part4.bank

/**
  * Created by kalit_000 on 5/21/19.
  */

/*

                                      product

                 deposits                                      lending
 checkings                       savings                       creditcards
corecheckings studentcheckings   RewardsSavings
*/



abstract class Product {
  val name:String

  override def toString: String = "prodcut=" + name

}

/*----------Deposits Products*/
abstract class Deposits extends Product {
  val interestRatePerYear:Double
  val minimalBalancePerMonth:Int
}

abstract class Checkings extends Deposits

abstract class Savings extends Deposits {
  val transactionAllowedPerMonth:Int
}

/*--------------checkingsProducts*/

class CoreChecking(bal:Int,rate:Double) extends Checkings {
  println("Created core checking product")
  override val interestRatePerYear:Double = rate
  override val minimalBalancePerMonth: Int = bal
  override val name: String = "Core Checking"
}

class StudentChecking(bal:Int,rate:Double) extends  Checkings {
  println("Created Student checking prodcut")
  override val minimalBalancePerMonth: Int = bal
  override val interestRatePerYear: Double = rate
  override val name: String = "Student Checking"
}

/*-------------------Savings Products -------------*/

class RewardSavings(bal:Int,rate:Double,trans:Int) extends Savings {
  println("Created Rewards Savings Product")
  override val minimalBalancePerMonth: Int = bal
  override val interestRatePerYear: Double = rate
  override val name: String = "Reward Savings"
  override val transactionAllowedPerMonth: Int = trans
}

/*-------------------Lending Products-------------*/

abstract class Lending extends Product {
  val annualFee:Double
  val apr:Double
  val rewardsPercent:Double
}

class CreditCard(fee:Double,rate:Double,pct:Double) extends Lending {
  println("Created Credit Card Products")
  override val annualFee:Double = fee
  override val apr:Double = rate
  override val rewardsPercent:Double = pct
  override val name:String = "Credit Card"
}







