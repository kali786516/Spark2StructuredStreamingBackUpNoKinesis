package com.dataframe.part4.part4.TraitExamples

/**
  * Created by kalit_000 on 5/23/19.
  */
import java.util.UUID
trait Db {

  private var contents:Map[String,String] = Map.empty

  protected def save(key:String, value: String): Unit =
    contents += (key -> value)

  def get(key:String):Option[String] = contents.get(key)
}

class Bank extends Db {
  def openAccount(userId:String):String = {
    val accountId = "A-" + UUID.randomUUID()
    save(userId,accountId)
    accountId
  }

  def getAccount(userId:String):Option[String] = get(userId)
}

/*
val bank = new Bank
val bonUserId = "U-BOB-" + UUID.randomUUID()
val amyUserID = "U-AMY-" _ UUID.randomUUID()

val bonAccountId = bank.openAccount(bobUserId)
val amyAccountId = bank.openAccount(amyUserId)

assert(bobAccountId == bank.getAccount(bobUserId).get,
  "Bob Account Id do not match")
assert(amyAccountId == bank.getAccount(amyUserId).get,
  "Amy Account Id do not match")
*/