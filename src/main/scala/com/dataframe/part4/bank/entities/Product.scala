package com.dataframe.part4.bank.entities

/**
  * Created by kalit_000 on 5/21/19.
  */

/*

                                      product

                 deposits                                      lending
 checkings                       savings                       creditcards
corecheckings studentcheckings   RewardsSavings
*/

import java.util.UUID

// sealed trait can be extended in the same file cannot be extened outside the file
// sealed trait are alternative to enums in many other languages

sealed trait ProductCategory
case object DepositsP extends ProductCategory
case object LendingP extends ProductCategory

abstract class Product {
  val id:UUID = UUID.randomUUID()
  val category: ProductCategory
  val name: String

  override def toString: String = "product=" + name

}

/*----------Deposits Products*/
abstract class Deposits extends Product {
  override val category = DepositsP
  val interestRatePerYear: Double
  val minimumBalancePerMonth: Dollars
}

abstract class Checkings extends Deposits

abstract class Savings extends Deposits {
  val transactionAllowedPerMonth:Int
}

/*--------------checkingsProducts*/
class CoreChecking(val minimumBalancePerMonth: Dollars,
                   val interestRatePerYear: Double) extends Checkings {
  println("Created core checking product")
  override val name: String = "Core Checking"
}

class StudentChecking(val minimumBalancePerMonth: Dollars,
                      val interestRatePerYear: Double) extends  Checkings {
  println("Created Student Checking Product")
  override val name: String = "Student Checking"
}

/*-------------------Savings Products -------------*/

class RewardSavings(val minimumBalancePerMonth: Dollars,
                    val interestRatePerYear: Double,
                    val transactionAllowedPerMonth: Int) extends Savings {
  println("Created Rewards Savings Product")
  override val name: String = "Rewards Savings"
}

/*-------------------Lending Products-------------*/

abstract class Lending extends Product {
  override val category = LendingP
  val annualFee: Double
  val apr: Double
  val rewardsPercent: Double
}

class CreditCard(val annualFee: Double,
                 val apr: Double,
                 val rewardsPercent: Double) extends Lending {
  println("Created Credit Card Products")
  override val name: String = "Credit Card"
}







