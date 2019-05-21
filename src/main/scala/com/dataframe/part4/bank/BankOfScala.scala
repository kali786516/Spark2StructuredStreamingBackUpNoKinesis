package com.dataframe.part4.bank

/**
  * Created by kalit_000 on 5/21/19.
  */

import java.time.LocalDate

object BankOfScala {
  def main(args: Array[String]): Unit = {
    println("Initiating Bank")

    val coreChecking = new CoreChecking(1000,0.025)
    val studentChecking = new StudentChecking(0,0.010)
    val rewardSavings = new RewardSavings(10000,0.10,1)
    val creditCard = new CreditCard(99.00,14.23,20.00)
    val products = Set(coreChecking,studentChecking,rewardSavings,creditCard)

    val bonMartin = new Customer("Bob","Martin","bob@martin.com",LocalDate.of(1983,8,22))
    val bobCheckingAccount = new Account(bonMartin,coreChecking,1000)
    val bonSavingsAccount = new Account(bonMartin,rewardSavings,20000)
    val bonCreditAccount = new Account(bonMartin,creditCard,4500)
    val accounts = Set(bobCheckingAccount,bonSavingsAccount,bonCreditAccount)

    val bank = new Bank("Bank of Scala","Auckland","New Zealand","bank@scala.com",products,Set(bonMartin),accounts)

    println(bobCheckingAccount)

    bobCheckingAccount.deposit(100)
    println(bobCheckingAccount)

    bobCheckingAccount.withdraw(200)
    println(bobCheckingAccount)

  }
}
