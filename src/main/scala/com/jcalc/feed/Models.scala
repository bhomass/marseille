package com.jcalc.feed

case class UserTransactions(transaction: SingleTransaction) {

  val customerId: String = transaction.customerId
  val tokens: String = transaction.tokens

}

case class SingleTransaction(rawFeed: String) {

  val segments = rawFeed.split(",")
  val customerId = segments(0)
  val transactionId = segments(1)
  val tokens = segments(2)

  def toBytes(): Array[Byte] = {
    (customerId + "," + transactionId + "." + tokens).toCharArray.map(_.toByte)
  }

}