package com.dataframe.part4.part4

/**
  * Created by kalit_000 on 5/20/19.
  */

import scala.concurrent.{Future,Await,Promise}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.{Success,Failure}
import scala.concurrent.duration._

case class TotalSalary(totalSalary:Double)

object ScalaConcurrency {
  def main(args: Array[String]): Unit = {

    val fut = Future { Thread.sleep(10000); 21 + 21}

    println(fut.isCompleted)

    fut.onComplete({
      case Success(result) => println("got the result" + result)
      case Failure(e) => println("failed")
    })

    val salalry = Future { Thread.sleep(200); 4000}

    val bonus = 500

    val salaryWithBonus = salalry.map(x => x + 500)

    var totoal = 0.0

    Await.result(salalry,2000 millis)

    println((salalry.isCompleted))

    var value = 0.0
    def total(salary:Double,bonus:Double):Double = {
       value = salary + bonus
       return value
    }

    var test = TotalSalary(0.0)

    if(salalry.isCompleted) {
      salalry.onComplete({
        case Success(result) => totoal = result + bonus
         //println(TotalSalary(result+bonus))
          println(result+bonus)
        case Failure(e) => println("no complete")
      })
    }

    println(test.totalSalary)

    val productPrice = Future {Thread.sleep((10000)); 150}

    val productTax = Future {Thread.sleep(10000);8.95}

    val finalPrice = for {
      price <- productPrice
      tax <- productTax
    } yield price + tax






  }

}
