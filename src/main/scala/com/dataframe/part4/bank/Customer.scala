package com.dataframe.part4.bank

/**
  * Created by kalit_000 on 5/21/19.
  */

import java.time.LocalDate

class Customer(f:String,l:String,e:String,dob:LocalDate) {
  val first:String = f
  val last:String = l
  val email:String = e
  val dateOfBirth:LocalDate = dob

  override def toString: String = first + "," + last

}
