package com.dataframe.part4

/**
  * Created by kalit_000 on 5/21/19.
  */
import scala.collection.mutable.Stack

object NineNineScalaProblems {
  def main(args: Array[String]): Unit = {

    val a = List(102,99,87,64,203,203,203)

    //last but one element
    //println(a.init.last)

    // last element
    //println(a.last)

    a.zipWithIndex.collect {
      case (x,i) if i == 3 => println(x)
    }

    val data = List("foo", "bar", "bash")
    val selection = List(0, 2)

    data.zipWithIndex.filter { case (data,index) => selection.contains(index)}.map(x => x._1)

    a.zipWithIndex.filter{case(data,index) => index == 3}.map(x => x._1)

    println(a.reverse)

    println(a.zipWithIndex.sortBy(x => x._2))

    println(a == a.reverse)

    println(a == a.zipWithIndex.sortBy(x => x._2).map(x => x._1))

    // List(102,99,87,64,203)

    println(a.zipWithIndex.sortBy(x => x._2).map(x => x._1))

    println(a.zipWithIndex.collect { case(data,index) => index})

    println(a.distinct)

    val b = List(1,1,1,4,4,4,3,5,2,5,2,3)

    println(b.flatMap{ e => List(e,e)})

    println(List.fill(3)("food"))

    println(List.tabulate(5)(x => x * x))

    println(b.grouped(3).flatMap(x => x.take(3-1)).toList)

    println(b.splitAt(3))

    println(b.zipWithIndex.filter{case(data,index) => index != 3}.map(x => x._1))

    val c = List(1,2,3,4,5)

    val (front,back)=c.splitAt(3)

    println(front:::List(101,102,103):::back)

    println(List.tabulate(6)(x => 4+x))

    println(scala.util.Random.shuffle(c.take(4)))

    val r = scala.util.Random

    val value = for (i <- 6 to r.nextInt(49)) yield i

    println(scala.util.Random.shuffle(value.take(6)))

   val value2= List('a, 'b, 'c, 'd, 'e, 'f)

    println(scala.util.Random.shuffle(value2.take(6)))

    println(a.map(x => (x)).groupBy(x => x))

    val listlength=List("kali","hari")

    println(listlength.map ( x => (x,x.length)).toList)

    println(listlength.foldLeft(0) { (count,_) => count+1})

    def sum(xs: List[Int]): Int = {
      xs match {
        case x :: tail => x + sum(tail) // if there is an element, add it to the sum of the tail
        case Nil => 0 // if there are no elements, then the sum is 0
      }
    }

    def avg(xs:List[Int]):Float={
      val (sum,length)=xs.foldLeft((0,0))( { case ((s,l),x)=> (x+s,1+l) })
      sum/length
    }

    println(avg(List(1,2,4,5,5,100)))

    println(List(1,2,4,5,5,100).map( x => x).reduce(_+_)/List(1,2,4,5,5,100).length)

    def gcd(x:Int,y:Int):Int = {
      if (x == 0 ) y
      else if ( y == 0) x
      else if ( x < y) gcd (x,y-x)
      else gcd(x-y,y)
    }

    val ints = Stack(100, 200, 3,4)

    ints.push(4)

    ints.pop()

    ints.top

    println(ints.map(x => x).min)

    println(ints.map(x => x).max)


    println(List(List(1,3),List(4,2)).flatMap(x => x diff (List(1,2))))

   Array.tabulate(4)(i => Array.tabulate(4)(j => i*4 + j))


    val array = List(1, 2, 100, 4, 5)

    val median = array.sortWith(_ < _).drop(array.length/2).head

    println(median)

    println(array.length)

    println(array.sortWith(_ < _))

    println(array.sortWith(_ < _).drop(array.length/2))

    val test = List("a","b","a","c","c","d","e","f","f")

    //println(test.map(x => (x,1)).countByKey())

   "GreenGrass".groupBy(c => c.toLower).map(e => (e._1, e._2.length)).toList.filter(x => x._2 == (1))



  }

}
