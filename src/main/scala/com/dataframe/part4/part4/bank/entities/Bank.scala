package com.dataframe.part4.part4.bank.entities

import com.dataframe.part4.part4.bank.entities
import com.dataframe.part4.part4.bank.services.{AccountService, CustomerService, ProductService, StatisticsService}

/**
  * Created by kalit_000 on 5/21/19.
  */

import java.util.UUID
import java.time.LocalDate

class Bank(val name:String, val city:String, val country:String, val email:Email)
  extends CustomerService with ProductService with AccountService with StatisticsService {

  /*
  private var depositProducts: Map[UUID,Deposits ] = Map.empty
  private var depositAccount: Map[UUID,DepositsAccount] = Map.empty
  private var lendingProducts:Map[UUID,Lending] = Map.empty
  private var lendingAccounts:Map[UUID,LendingAccount] = Map.empty
  private var customers:Map[UUID,Customer] = Map.empty

  println(s"${name} Established 2018")

  /**./
    * create New Customer and return UUID
    * @param first
    * @param last
    * @param email
    * @param dateofBirth
    * @return UUID
    */
  def createNewCustomer(first:String,last:String,email: String,dateofBirth:String):UUID = {

    def getEmail:Email = {
      val Array(value,domin) = email.split("@")
      Email(value,domin)
    }

    def getDateOfBirth:LocalDate = {
      val Array(year,month,day) = dateofBirth.split("/")
      LocalDate.of(year.toInt,month.toInt,day.toInt)
    }

    val customer = new Customer(first,last,getEmail,getDateOfBirth)
    customers += (customer.id -> customer)
    customer.id
  }

  /**
    * create  and return UUID
    * @param name
    * @param minBalance
    * @param ratePerYear
    * @param transactionAllowedPerMonth
    * @return
    */
  def addNewDepositProdcut(name:String,minBalance:Int,ratePerYear:Double,
                          transactionAllowedPerMonth:Int = 2):UUID ={

    val product = name match {
      case "CoreChecking" => new CoreChecking(Dollars(minBalance),ratePerYear)
      case "StudentCheckings" => new StudentChecking(Dollars(minBalance),ratePerYear)
      case "RewardsSavings" => new RewardSavings(Dollars(minBalance),ratePerYear, transactionAllowedPerMonth)
    }

    depositProducts += (product.id -> product)
    product.id
   }

  /**
    *
    * @param annualFee
    * @param apr
    * @param rewardsPercent
    * @return
    */
    def addNewLendingProdcut(annualFee:Double,apr:Double,rewardsPercent:Double):UUID = {
    val product = new CreditCard(annualFee,apr,rewardsPercent)
    lendingProducts += (product.id -> product)
    product.id
  }

  def openDepositAccount(customerId:UUID,productId:UUID,amount:Dollars):UUID ={
    require(customers.get(customerId).nonEmpty,s"no customer found with id=$customerId")
    require(depositProducts.get(productId).nonEmpty,s"no of deposits product found with id=$productId")

    val account = new DepositsAccount(customers(customerId),depositProducts(productId),amount)
    depositAccount += (account.id -> account)
    account.id
  }

  def openLendingAccount(customerId:UUID,productId:UUID,balance:Dollars = Dollars(0)):UUID = {
    require(customers.get((customerId)).nonEmpty,s"no customer found with id=$customerId")
    require(lendingProducts.get(productId).nonEmpty,s"no of lending product found with id=$productId")

    val account = new LendingAccount(customers(customerId),lendingProducts(productId), balance)
    lendingAccounts += (account.id -> account)
    account.id
  }

  def deposit(accountId:UUID,dollars: Dollars) = {
    require(depositAccount.get(accountId).nonEmpty,"A valid deposits account Id must be provided")
    depositAccount(accountId) deposit dollars
  }

  def withdraw(accountId:UUID,dollars: Dollars) = {
    require(depositAccount.get(accountId).nonEmpty,"A valid deposits account Id must be provided")
    depositAccount(accountId) withdraw dollars
  }

  def useCreditCard(accountId:UUID,dollars: Dollars) = {
    require(lendingAccounts.get(accountId).nonEmpty, "A valid lending account Id must be provided")
    lendingAccounts(accountId) withdraw dollars
  }

  def payCreditCardBill(accountId:UUID,dollars: Dollars) ={
    require(lendingAccounts.get(accountId).nonEmpty,"A Valid lending account Id must be provided")
    lendingAccounts(accountId) payBill dollars
  }

 */

  println(s"$name Established 2018.")

  override def toString: String = s"[$name]" +
  s" - ${numCustomers} customers" +
  s" - ${numDepositsProducts} deposits products" +
  s" - ${numDepositsAccounts} deposits accounts" +
  s" - ${numLendingProducts} lending products" +
  s" - ${numLendingAccounts} lending accounts"
}
