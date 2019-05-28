package com.dataframe.part4.bank.services

/**
  * Created by kalit_000 on 5/24/19.
  */
import java.time.LocalDate
import java.util.UUID

import com.dataframe.part4.bank.entities._

trait CustomerService extends CustomerDb {

  /**
    * this method takes in all details of customer and saves to database by calling trait CustomerDB and returns back with UUID value
    * @param first
    * @param last
    * @param email
    * @param dateOfBirth
    * @return
    */

  def createNewCustomer(first:String,last:String,
                       email:String,dateOfBirth:String):UUID = {

    def getEmail:Email = {
      val Array(value,domain) = email.split("@")
      Email(value,domain)
    }

    def getDateOfBirth:LocalDate = {
      val Array(year,month,day) = dateOfBirth.split("/")
      LocalDate.of(year.toInt,month.toInt,day.toInt)
    }

    val customer = new Customer(first,last,getEmail,getDateOfBirth)
    // saveCustomer method coming from CustomerDB trait
    saveCustomer(customer)
    customer.id
  }

}
