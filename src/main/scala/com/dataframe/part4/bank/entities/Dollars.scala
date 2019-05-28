package com.dataframe.part4.bank.entities

/**
  * Created by kalit_000 on 5/23/19.
  */

//companion object which helps without using new keyword
object Dollars {
  val Zero = new Dollars(0)
  def apply(a:Int): Dollars = new Dollars(a)
}

class Dollars(val amount:Int)extends AnyVal with Ordered[Dollars] {
  override def compare(that: Dollars): Int = amount - that.amount

  //unary operator

  def +(dollars: Dollars):Dollars = new Dollars(amount + dollars.amount)

  def -(dollars: Dollars):Dollars = new Dollars(amount - dollars.amount)

  //def >(dollars: Dollars):Boolean = amount > dollars.amount

  override def toString: String = "$" + amount
}