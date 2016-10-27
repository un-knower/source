package com.boc.iff.exception

/**
  * Created by birdie on 9/18/16.
  */
sealed trait BaseException {
}

case class RecordNumberErrorException(val message:String) extends Exception(message)

case class MaxErrorNumberException(val message:String) extends Exception(message)

case class RecordNotFixedException(val message:String) extends Exception(message)

case class MaxBlankNumberException(val message:String) extends Exception(message)

case class PrimaryKeyMissException(val message:String) extends Exception(message)

