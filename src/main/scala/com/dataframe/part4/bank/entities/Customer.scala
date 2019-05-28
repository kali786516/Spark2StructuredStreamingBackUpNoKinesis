package com.dataframe.part4.bank.entities

/**
  * Created by kalit_000 on 5/21/19.
  */

import java.time.LocalDate
import java.util.UUID

class Customer(val first:String,val last:String,val email:Email,val dateOfBirth:LocalDate) {
  val id:UUID = UUID.randomUUID()
  override def toString: String = s"$id -> $first,$last,$email"
}
