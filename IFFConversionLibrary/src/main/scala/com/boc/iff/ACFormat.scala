package com.boc.iff

import java.text.{ParsePosition, FieldPosition, Format}

/**
  * Created by cvinc on 2016/8/9.
  */
class ACFormat(pattern: String) extends Format with Serializable{

  val padStr = pattern.substring(0, 1)
  val maxChars = Integer.parseInt(pattern.substring(1, pattern.length()))

  def format(s: String): String = {
    format(s, new StringBuffer(), null).toString
  }

  protected final def pad(to: StringBuffer, howMany: Int): Unit = {
    for (i <- 0 until howMany) {
      to.append(padStr)
    }
  }

  override def parseObject(source: String, pos: ParsePosition): AnyRef = null

  override def format(obj: scala.Any, toAppendTo: StringBuffer, pos: FieldPosition): StringBuffer = {
    val text = obj.asInstanceOf[String].replaceAll("^[ ]*[+-]", "")
    pad(toAppendTo, maxChars - text.length)
    toAppendTo.append(text)
    if (toAppendTo.length() > maxChars) {
      toAppendTo.delete(0, toAppendTo.length - maxChars)
    }
    //sb.insert(0,' ');
    toAppendTo
  }
}
