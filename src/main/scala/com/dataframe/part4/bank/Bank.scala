package com.dataframe.part4.bank

/**
  * Created by kalit_000 on 5/21/19.
  */
class Bank(n:String,c:String,co:String,e:String,ps:Set[Product],cs:Set[Customer],as:Set[Account]) {
  println("$n Established 2018")

  val name = n
  val city = c
  val country = co
  val email = e
  val products:Set[Product] = ps
  val customer:Set[Customer] = cs
  val accounts:Set[Account] = as

}
