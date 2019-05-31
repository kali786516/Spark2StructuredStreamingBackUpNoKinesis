package com.dataframe.part4.part4

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

/**
  * Created by kalit_000 on 5/20/19.
  */

case class Fruit(name:String)
case class Book(title:String,yearPublished:Integer,author:String,isbn:String)
case class Trip(to:String)
case class Car(model:String)
case class Cash(amount:Integer)
case class NoPrize()
case class Event(location:String,dayOfWeek:String,sessionTimeInSeconds:Int,source:String)

object ScalaBasics {
  def main(args: Array[String]): Unit = {
    val logger = Logger.getLogger("HbIngestion")
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)
    val startTimeMillis = System.currentTimeMillis()

    val spark=SparkSession.builder()
      .appName("Test2").master("local").config("spark.driver.memory","2g").enableHiveSupport().getOrCreate()

    import spark.implicits._

    val apple = Fruit("apple")
    val orange = Fruit("orange")
    val kiwi = Fruit("kiwi")

    val fruits=List(apple,orange,kiwi)

    def getApples(searchFruit:List[Fruit]) = {
      for (fruit<- searchFruit if fruit.name == "apple") yield fruit
    }

    def getOranges(seachOranges:List[Fruit]) = {
      for (fruit <- seachOranges if fruit.name == "orange") yield fruit
    }

    def getFruits(basket:List[Fruit],filter:Fruit => Boolean)={
      for (fruit <- basket if filter(fruit)) yield fruit
    }

    println(getApples(fruits))

    println(getFruits(fruits,(fruit:Fruit) => fruit.name == "apple"))

    val arguments = Array("Monday","Tuesday")


    def checkArray(value:String) = {
      if (!arguments.isEmpty) {
        val location=arguments.indexOf(value)
        arguments(location)
      }
    }

    println(checkArray("Monday"))

    val letters = List("a","b","c")
    val numbers = List(1,2)

    for (number <- numbers){
      for (letter <- letters){
        println(number + " => " + letter)
      }
    }


    val result=for (number <- numbers) yield number * 2

    println(result)

    def product(a:Int,b:Int) = a*b

    val multiply = (a:Int,b:Int) => a*b

    println(multiply(5,4))


    def stringToInt(in:String): Either[String,Int] ={
      try {
        Right(in.toInt)
      }
      catch {
        case e:NumberFormatException => Left("Error: " + e.getMessage)
      }
    }

    val fiveToInt = stringToInt("5")

    val helloToInt = stringToInt("hello").left

    println(helloToInt)
    println(fiveToInt)

    def test(value:String) = stringToInt(value) match {
      case Right(x) => println("all good")
      case Left(x) => println("very bad")
    }

    println(test("kali"))

    val progInScala = Book("Programming in scala",2016,"Martin","12e32112")

    val funcProgInScala = Book("Scala shortcuts",2019,"Kali Tummala","121e13212")

    def findAuthor(value:String,objectValue:Book)=objectValue match {
      case Book(_,_,author,_) => author.equals(value) match
      {
        case true => println("values match")
        case _ => println("values dont match")
      }
      case _ => None
    }

    findAuthor("kali",Book("Programming in scala",2016,"Martin","12e32112"))

    findAuthor("kali tummala",Book("Scala shortcuts",2019,"Kali Tummala","121e13212"))


    def findInList(value:Int,input:List[Int]) ={
      input.contains(value) match {
        case true => input(input.indexOf(value))
        case _ =>
      }
    }

    println(findInList(3,List(1,2,3,4)))


    val numbersList = List(10,20,30,40,50,60)

    numbersList match {
      case List(a,b,c) => b
      case _ => -1
    }

    numbersList match {
      case List(_,second,_*) => second
      case _ => -1
    }

    val magicBucket = List(NoPrize(),Car("Tesla"),Trip("New Zealand"),Trip("califorania"),NoPrize,Cash(500))

    import scala.util.Random

    Random.shuffle(magicBucket).take(1)take(0) match {
      case t: Trip => println("you won" + t.to)
      case c: Car => println("you won" + c.model)
      case ca: Cash => println("you won" + ca.amount)
      case _ => println("you won nothing")
    }

    magicBucket(scala.util.Random.nextInt(magicBucket.size))

   val event1=Event("US","Sun",10,"Twitter")
   val event2 = Event("Chine","Mon",15,"wechat")

    val events = List(event1,event2)

    val locations = events.map(x => x.location)

    def toInt(s:String):Option[Int] ={
      try {
        Some(s.toInt)
      } catch {
        case e: NumberFormatException => None
      }
    }

    val arguments1 = List("10","eight","5","four","3","20")

    println(arguments1.map(x => toInt(x)))

    println(arguments1.flatMap(x => toInt(x)))

    //println(arguments1.flatMap(x => toInt(x)).sum)



  }

}
