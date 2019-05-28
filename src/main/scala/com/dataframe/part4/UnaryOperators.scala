package com.dataframe.part4

/**
  * Created by kalit_000 on 5/23/19.
  */
object UnaryOperators {
  def apply(value:String):UnaryOperators = new UnaryOperators(value)
}

class UnaryOperators(val string:String){
  def unary_! = new UnaryOperators(string + "!!")
  override def toString: String = string
}


