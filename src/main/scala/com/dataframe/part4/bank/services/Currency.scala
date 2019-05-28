package com.dataframe.part4.bank.services

import com.dataframe.part4.bank.entities.Dollars

/**
  * Created by kalit_000 on 5/25/19.
  */



object Currency {

  private val forexRates:Map[String,Double] = Map(
    "CAD" -> 0.9033746787,
    "USD" -> 0.6878347389,
    "EUR" -> 0.5990840809,
    "SGD" -> 0.9324354523,
    "INR" -> 48.898989898,
    "NZD" -> 1.0
  )

  implicit def stringToCurrency(money:String):Currency = {
    val Array(value:String,code:String) = money.split("\\s")
    val requestedAmount:Double = value.toDouble
    val currencyFactor:Double = forexRates(code)
    val totalCost: Int = (requestedAmount / currencyFactor).toInt

    println(s"Cost of converting $requestedAmount $code -> $totalCost NZD")
    Currency(code,value.toDouble,Dollars(totalCost))

  }

}

case class Currency(code:String,amount:Double,costInDollars:Dollars)