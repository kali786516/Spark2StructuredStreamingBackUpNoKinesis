package com.dataframe.part4.bank.entities

/**
  * Created by kalit_000 on 5/23/19.
  */

//companion object which helps without using new keyword
object Email {
  def apply(value:String,domain:String):Email = new Email(value,domain)
}

class Email(val value:String,val domain:String){
  override def toString: String = s"$value@$domain"
}


