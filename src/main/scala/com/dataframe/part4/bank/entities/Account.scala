package com.dataframe.part4.bank.entities

/**
  * Created by kalit_000 on 5/21/19.
  */

import java.util.UUID


// sealed trait can be extended in the same file cannot be extened outside the file
// sealed trait are alternative to enums in many other languages

sealed trait TransactionType
// In Presents money coming into the bank
case object In extends TransactionType
// Out Represents money going out of the bank
case object Out extends TransactionType

case class Transaction(customer: Customer,amount:Dollars,
                      transactionType: TransactionType,accountCategory:AccountCategory)

sealed trait AccountCategory
case object DepositsA extends AccountCategory
case object LendingA extends AccountCategory

abstract class Account{
  val id:UUID = UUID.randomUUID()
  val customer:Customer
  val product:Product
  val category: AccountCategory
  var transactions: Seq[Transaction] = Seq.empty

  def getBalance: Dollars
}

class DepositsAccount(val customer: Customer,val product: Deposits,
                     private var balance: Dollars) extends Account {

  override val category: AccountCategory = DepositsA

  def deposit(dollars:Dollars) = {
    require(dollars > Dollars(0) ,"amount deposited should be greater than Zero. ")
    println(s"Depositing $dollars to $customer account")
    balance += dollars
    transactions = transactions :+ Transaction(customer,dollars,In,category)

    println(s"Deposited $dollars to ${this.toString}")
  }

  def withdraw(dollars:Dollars) ={
    require(dollars > Dollars(0) && balance > dollars , "amount should be greater than zero and requested amount should be less than or equal to balance.")
    println(s"Withdrwing $dollars to $customer account")
    balance -= dollars
    println(s"Withdrawn $dollars to ${this.toString}")
  }

  override def getBalance: Dollars = balance

  override def toString: String = s"$customer with $product has reamining balnce of $balance"

}

class LendingAccount(val customer:Customer,val product:Lending,
                     private var balance:Dollars) extends Account {

  override val category: AccountCategory = LendingA

  def payBill(dollars:Dollars) ={
    require(dollars > Dollars(0), "the payment must be made for amount greater than Zero.")
    println(s"Paying Bill of $dollars againtst $customer account")
    balance += dollars
    transactions = transactions :+ Transaction(customer,dollars,In,category)
    println(s"Paid bill of $dollars against ${this.toString}")
  }

  def withdraw(dollars:Dollars) ={
    require(dollars > Dollars(0) , "The withdrawal amount must be greater than Zero")
    println(s"debating $dollars from $customer account")
    transactions = transactions :+ Transaction(customer,dollars,Out,category)
    balance -= dollars
  }

  override def getBalance: Dollars = balance

  override def toString: String = s"$customer with $product has remaining balance of $balance"


}
