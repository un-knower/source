package com.boc.iff

import java.text.{ParsePosition, FieldPosition, Format}

/**
  * Created by cvinc on 2016/8/9.
  */
class StringAlign(pattern: String) extends Format with Serializable{

  /* Constant for left justification. */
  val JUST_LEFT: Int = 'L'
  /* Constant for centering. */
  val JUST_CENTRE: Int = 'C'
  /* Centering Constant, for those who spell "centre" the American way. */
  val JUST_CENTER: Int = JUST_CENTRE
  /* Constant for right-justified Strings. */
  val JUST_RIGHT: Int = 'R'

  val padStr = pattern.substring(0, 1)
  /** Current justification */
  val just = pattern.substring(pattern.length() - 1, pattern.length()).toUpperCase().getBytes().head.toInt
  /** Current max length */
  val maxChars = Integer.parseInt(pattern.substring(1, pattern.length() - 1))

  just match {
    case JUST_LEFT =>
    case JUST_CENTER =>
    case JUST_RIGHT =>
    case _ => throw new IllegalArgumentException("invalid justification arg.");
  }

  if (maxChars < 0) {
    throw new IllegalArgumentException("maxChars must be positive.");
  }

  /** Convenience Routine */
  def format(s: String): String = {
    format(s, new StringBuffer(), null).toString
  }

  protected final def pad(to: StringBuffer, howMany: Int): Unit = {
    for (i <- 0 until howMany) {
      to.append(padStr)
    }
  }

  /** ParseObject is required, but not useful here. */
  override def parseObject(source: String, pos: ParsePosition): AnyRef = source


  /** Format a String.
    *
    * @param obj        - the string to be aligned.
    * @param toAppendTo - the StringBuffer to append it to.
    * @param pos        - a FieldPosition (may be null, not used but
    *                   specified by the general contract of Format).
    */
  override def format(obj: scala.Any, toAppendTo: StringBuffer, pos: FieldPosition): StringBuffer = {
    val text = obj.asInstanceOf[String]
    val wanted = text.substring(0, Math.min(text.length, maxChars))

    // Get the spaces in the right place.
    just match {
      case JUST_RIGHT =>
        pad(toAppendTo, maxChars - wanted.length)
        toAppendTo.append(wanted)
      case JUST_CENTRE =>
        val toAdd = maxChars - wanted.length
        pad(toAppendTo, toAdd / 2)
        toAppendTo.append(wanted)
        pad(toAppendTo, toAdd - toAdd / 2)
      case JUST_LEFT =>
        toAppendTo.append(wanted)
        pad(toAppendTo, maxChars - wanted.length)
    }
    toAppendTo
  }
}
