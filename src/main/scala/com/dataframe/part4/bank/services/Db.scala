package com.dataframe.part4.bank.services

/**
  * Created by kalit_000 on 5/24/19.
  */

import java.util.UUID
import com.dataframe.part4.bank.entities._

trait CustomerDb {
  private var customers:Map[UUID,Customer] = Map.empty
  def saveCustomer(customer: Customer) = customers += (customer.id -> customer)
  def getCustomer(id:UUID):Option[Customer] = customers.get(id)
  def numCustomers:Int = customers.size
}

trait ProductsDb {
  private var depositProducts:Map[UUID,Deposits] = Map.empty
  private var lendingProducts:Map[UUID,Lending] = Map.empty

  def saveDepositProduct(product: Deposits) = depositProducts += (product.id -> product)

  def saveLendingProduct(product: Lending) = lendingProducts += (product.id -> product)

  def getDepositProduct(id:UUID):Option[Deposits] = depositProducts.get(id)
  def getLendingProduct(id:UUID):Option[Lending] = lendingProducts.get(id)
  def numDepositsProducts:Int = depositProducts.size
  def numLendingProducts:Int = lendingProducts.size
}

trait AccountsDb {
  private var depositsAccount: Map[UUID,DepositsAccount]  = Map.empty
  private var lendingAccount: Map[UUID,LendingAccount] = Map.empty

  def saveDepositsAccount(account: DepositsAccount) = depositsAccount += (account.id -> account)
  def saveLendingAccount(account: LendingAccount) = lendingAccount += (account.id -> account)
  def getDepositAccount(id:UUID):Option[DepositsAccount] = depositsAccount.get(id)
  def getLendingAccount(id:UUID):Option[LendingAccount] = lendingAccount.get(id)
  def numDepositsAccounts: Int = depositsAccount.size
  def numLendingAccounts: Int = lendingAccount.size
}
