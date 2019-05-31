package com.dataframe.part4.part4.bank

/**
  * Created by kalit_000 on 5/21/19.
  */

import java.time.LocalDate
import java.util.UUID

import com.dataframe.part4.part4.UnaryOperators
import com.dataframe.part4.part4.bank.entities._

object BankOfScala {
  val random = new scala.util.Random()

  def main(args: Array[String]): Unit = {
    println("Opening Bank")

    /* ------------------- Data ------------------- */
    def getCustomers: Seq[(String, String, String, String)] = {
      Seq(
        ("Bob", "Martin", "bob@martin.com", "1976/11/25"),
        ("Amy", "Jones", "amy.jones@google.com", "1983/4/12"),
        ("Taylor", "Jackson", "taylor33@jackson.com", "1985/4/5"),
        ("Rob", "Johnson", "rob@jphnson.com", "1976/3/6"),
        ("Maggie", "Harting", "maggie23@apple.com", "1979/12/3"),
        ("Deb", "Das", "debdas@ge.com", "1981/4/12"),
        ("Chris", "Baron", "chris@ibm.com", "1977/2/12"),
        ("Julie", "James", "julie@james.com", "1983/1/22"),
        ("Oscar", "Chin", "oscar@mama.com", "1982/4/12"),
        ("Sophie", "Smith", "sophie@fb.com", "1990/12/1")
      )
    }

    def getDepositProducts: Seq[(String, Int, Double)] = {
      Seq(
        ("CoreChecking", 1000, 0.025),
        ("StudentCheckings", 0, 0.010),
        ("RewardsSavings", 10000, 0.10)
      )
    }

    def getLendingProducts: Seq[(String, Double, Double, Double)] = {
      Seq(("CreditCard", 99.00, 14.23, 20.00))
    }

    val bank = new Bank(name = "Bank of Scala", country = "New Zealand", city = "Auckland", email = Email("bank", "scala.com"))
    val customerIds = getCustomers map { c => bank.createNewCustomer(c._1, c._2, c._3, c._4) }
    val depositProductIds = getDepositProducts map { p => bank.addNewDepositProduct(p._1, p._2, p._3) }
    val lendingProductIds = getLendingProducts map { l => bank.addNewLendingProduct(l._2, l._3, l._4) }

    /* logging */
    println(s"Bank: $bank")
    println(s"CustomerIds: $customerIds")
    println(s"Deposits Products Ids: $depositProductIds")
    println(s"LendingProductIds: $lendingProductIds")

    def openAccounts(customerId: UUID, product: Product) = product.category match {
      case DepositsP => bank.openDepositAccount(customerId, product.id, _: Dollars)
      case LendingP => bank.openLendingAccount(customerId, product.id, _: Dollars)
    }

    /*
    Bank clerk opening the account.
    There is no money deposited in the account yet, so the accounts are not active
    */
    val depositAccounts = for (c <- customerIds; p <- depositProductIds) yield openAccounts(c, bank.getDepositProduct(p).get)
    /* Depositing money into the accounts */
    val depositAccountIds = depositAccounts map { account => account(Dollars(10000 + random.nextInt(10000))) }


    /* logging */
    println(s"Deposits Accounts: $depositAccounts")
    println(s"Deposits Account Ids: $depositAccountIds")

    /*
     Open credit card accounts.
     The bank process has not finished the credit check, so, balance will be known later
    */
    val lendingAccounts = for (c <- customerIds; p <- lendingProductIds) yield openAccounts(c, bank.getLendingProduct(p).get)
    val lendingAccountIds = lendingAccounts map { account => account(Dollars(random.nextInt(500))) }

    /* logging */
    println(s"Lending Accounts: $lendingAccounts")
    println(s"Lending Account Ids: $lendingAccountIds")
    println(s"Bank: $bank")

    /*
      Performing Deposit Accounts transactions
     */
    depositAccountIds foreach { accountId =>
      bank deposit(accountId, Dollars(1 + random.nextInt(100)))
      bank withdraw(accountId, Dollars(1 + random.nextInt(50)))
    }

    /*
      Performing Lending Accounts transactions
    */
    lendingAccountIds foreach { accountId =>
      bank useCreditCard(accountId, Dollars(10000 + random.nextInt(500)))
      bank payCreditCardBill(accountId, Dollars(1000 + random.nextInt(100)))
    }

    /* Request Currency */
    println("-" * 100)
    println("Requesting Currency")
    bank.requestCurrency(depositAccountIds.head, "120 USD")
    bank.requestCurrency(depositAccountIds.tail.head, "100 CAD")

    /** Statistics **/
    println("-" * 100)
    println("Statistics")
    println("-" * 100)

    val dAccounts = depositAccountIds flatMap {bank.getDepositAccount}
    println(s"Total Money Deposited to Bank: ${bank.getTotalMoneyDeposited(dAccounts)}")

    val lAccounts = lendingAccountIds flatMap {bank.getLendingAccount}
    println(s"Total Money Borrowed by Customers: ${bank.getTotalMoneyBorrowedByCustomers(lAccounts)}")

    val allAccounts = dAccounts ++ lAccounts
    println(s"Number of Transactions By Account: ${bank.getNumTransactionsByAccount(allAccounts)}")

    /* Version 1
    /*....................step 1 set products of banks and its rates........................*/
    val coreChecking = new CoreChecking(Dollars(1000),0.025)
    val studentChecking = new StudentChecking(Dollars(0),0.010)
    val rewardSavings = new RewardSavings(Dollars(10000),0.10,1)
    val creditCard = new CreditCard(99.00,14.23,20.00)
    val products = Set(coreChecking,studentChecking,rewardSavings,creditCard)

    /*....................step 2 create Customer........................*/
    val bonMartin = new Customer("Bob","Martin",Email("bob","martin.com"),LocalDate.of(1983,8,22))

    /*....................step 3 create bank accounts for customer in different bank products........................*/
    val bobCheckingAccount = new Account(bonMartin,coreChecking,Dollars(1000))
    val bonSavingsAccount = new Account(bonMartin,rewardSavings,Dollars(20000))
    val bonCreditAccount = new Account(bonMartin,creditCard,Dollars(4500))

    /*....................step 4 set accounts for one customer........................*/
    val accounts = Set(bobCheckingAccount,bonSavingsAccount,bonCreditAccount)

    /*....................step 5 set bank........................*/
    val bank = new Bank("Bank of Scala","Auckland","New Zealand",Email("bank","scala.com"),products,Set(bonMartin),accounts)

    /*....................step 6 get customer checking account details........................*/
    println(bobCheckingAccount)

    /*....................step 7 deposit some amount........................*/
    bobCheckingAccount deposit 100
    println(bobCheckingAccount)

    /*....................step 8 withdraw some amount........................*/
    bobCheckingAccount withdraw 200
    println(bobCheckingAccount)

    */


 }
}
