package com.dataframe.part4.bank.services

/**
  * Created by kalit_000 on 5/24/19.
  */
import com.dataframe.part4.bank.entities._

trait StatisticsService {

  // sql column  and where condition
    def getTotalMoneyDeposited(accounts:Seq[Account]):Dollars = {
    accounts.foldLeft(Dollars.Zero)((total,account) => if (account.category == DepositsA) total + account.getBalance else total)
  }

  def getTotalMoneyBorrowedByCustomers(accounts:Seq[Account]):Dollars = {
    //println(accounts.foldLeft(Dollars.Zero)((total,account) => if (account.category == LendingA) total + account.getBalance else total))
    accounts map { a => if (a.category == LendingA) a.getBalance else Dollars.Zero } reduce(_ + _)
  }

  def getNumTransactionsByAccount(accounts:Seq[Account]):Map[String,Int] = {
    //implicit def sum[B >: Int](num: Numeric[Int])
    // Seq[(AccountCategory,NumberOfTrasactionsIntheAccount)]
    val tuples: Seq[(AccountCategory,Int)] = accounts.map { a => a.category -> a.transactions.length }

    println("tuples  kali ........."+tuples) //(DepositsA,1), (DepositsA,1)

    println("tuples.groupBy(_._1)  kali ........."+tuples.groupBy(_._1))

    //Map(LendingA -> List((LendingA,2), (LendingA,2), (LendingA,2), (LendingA,2), (LendingA,2),
    // (LendingA,2), (LendingA,2), (LendingA,2), (LendingA,2), (LendingA,2))

    //performs group by on these tuple based on first value key is account category and value is of same type seq element of tuples
    // number of transactions by transaction type
    val categoryToTuples: Map[AccountCategory, Seq[(AccountCategory,Int)]] = tuples.groupBy(_._1)
    categoryToTuples map {

      case(accountCategoty,rest) =>
        println("account category kali ........."+accountCategoty.toString)
        println("rest kali ........."+rest.toString)
        accountCategoty.toString -> rest.map(_._2).sum

    }
   }

}
