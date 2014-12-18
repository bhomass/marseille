package com.jcalc.feed

import kafka.serializer.Decoder
import kafka.serializer.Encoder
import kafka.utils.VerifiableProperties

class SingleTransactionDecoder(props: VerifiableProperties) extends Decoder[SingleTransaction] {
  def fromBytes(bytes: Array[Byte]): SingleTransaction = {
     new SingleTransaction(new String(bytes))
  }
}

class SingleTransactionEncoder(props: VerifiableProperties) extends Encoder[SingleTransaction] {
  def toBytes(singleTransaction: SingleTransaction): Array[Byte] = {
    singleTransaction.toBytes()
  }
}