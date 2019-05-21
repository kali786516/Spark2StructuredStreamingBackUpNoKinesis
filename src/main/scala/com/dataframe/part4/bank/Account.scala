package com.dataframe.part4.bank

/**
  * Created by kalit_000 on 5/21/19.
  */
class Account(c:Customer,p:Product,b:Int) {
  val customer:Customer = c
  val product:Product = p
  private var balance:Int = b

  def deposit(amount:Int) = {
    println(s"Depositing $amount to $customer account")
    balance += amount
  }

  def withdraw(amount:Int) = {
    println(s"Withdrwaing $amount to $customer account")
    balance -= amount
  }

  override def toString: String = s"$customer with $product has remaining balance of $balance"
}
