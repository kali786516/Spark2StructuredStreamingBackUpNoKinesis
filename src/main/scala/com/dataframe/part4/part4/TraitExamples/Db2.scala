package com.dataframe.part4.part4.TraitExamples

/**
  * Created by kalit_000 on 5/23/19.
  */
trait Db2 {

  private var contents:Map[String,String] = Map.empty

  def save(key:String,value:String) = contents += (key -> value)

  def get(key:String): Option[String] = contents.get(key)

}

class blah
class InMeomry2 extends blah with Db2

/*
val repo = InMemory2

repo.save("a","apple")
repo.save("b","banna")

repo.get("a")

*/