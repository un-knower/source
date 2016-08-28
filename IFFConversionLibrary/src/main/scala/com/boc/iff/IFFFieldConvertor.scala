package com.boc.iff

import java.math.BigInteger
import java.nio.charset.CharsetDecoder
import java.text.DecimalFormat

import com.boc.iff.model._
import com.ibm.as400.access.AS400ZonedDecimal
import org.apache.commons.lang3.StringUtils

/**
  * Created by cvinc on 2016/6/23.
  */

@annotation.implicitNotFound(msg = "No implicit IFFFieldConvertor defined for ${T}.")
sealed trait IFFFieldConvertor[T<:IFFFieldType] {
  //为什么需要这2个函数？

  protected def convertFromByte(fieldType: T, fieldBytes: Array[Byte], decoder: CharsetDecoder): Any = null
  protected def convertFromString(fieldType: T, fieldValue: String): Any = fieldValue
  def convert(fieldType: T, fieldBytes: Array[Byte], decoder: CharsetDecoder): String = {
    val (needByteDecode, needLineEsc) = fieldType match {
      case fieldType: IFFByteDecodeWithLineEsc => (true, true)
      case fieldType: IFFByteDecodeWithOutLineEsc => (true, false)
      case _ => (false, false)
    }
    val convertedFieldValue = needByteDecode match {
      case true =>
        val decodedValue = IFFUtils.byteDecode(decoder, fieldBytes, needLineEsc)
        convertFromString(fieldType, decodedValue)
      case _ => convertFromByte(fieldType, fieldBytes, decoder)
    }
    val formattedFieldValue = fieldType match {
      case fieldType: IFFFormatable if fieldType.formatSpec != null =>
        fieldType.formatSpec.getFormatObj.format(convertedFieldValue)
      case _ => convertedFieldValue.toString
    }
    val trimedFieldValue = fieldType match {
      case fieldType: IFFNeedTrim if StringUtils.isNotEmpty(formattedFieldValue) =>
        formattedFieldValue.trim
      case _ => formattedFieldValue
    }
    trimedFieldValue.toString
  }
}

object IFFFieldConvertor {

  trait IFFDateFieldConvertor extends IFFFieldConvertor[IFFDate] {
    override def convertFromString(fieldType: IFFDate,fieldValue: String): String = {
      if (fieldValue.length == 10) {
        fieldValue.replaceAll("0000/00/00|0000-00-00", "          ")
      } else {
        fieldValue.replaceAll("00000000|[\\.]      ", "        ")
      }
    }
  }
  implicit object IFFDateField extends IFFDateFieldConvertor

  trait IFFTimeFieldConvertor extends IFFFieldConvertor[IFFTime] {
    override def convertFromString(fieldType: IFFTime,fieldValue: String): String = {
      if (fieldValue.length == 6) {
        "%s:%s:%s.000".format(fieldValue.substring(0, 2),
          fieldValue.substring(2, 4),fieldValue.substring(4, 6))
      } else {
        fieldValue + ".000"
      }
    }
  }
  implicit object IFFTimeField extends IFFTimeFieldConvertor

  trait IFFTimestampFieldConvertor extends IFFFieldConvertor[IFFTimestamp] {
    override def convertFromString(fieldType: IFFTimestamp,fieldValue: String): String = {
      if(StringUtils.isNotBlank(fieldValue)){
        "%s %s:%s:%s".format(fieldValue.substring(0, 10),
          fieldValue.substring(10, 12),
          fieldValue.substring(12, 14),
          fieldValue.substring(14, 16))
      }else fieldValue
    }
  }
  implicit object IFFTimestampField extends IFFTimestampFieldConvertor

  trait IFFStringFieldConvertor extends IFFFieldConvertor[IFFString]
  implicit object IFFStringField extends IFFStringFieldConvertor

  trait IFFUStringFieldConvertor extends IFFFieldConvertor[IFFUString]
  implicit object IFFUStringField extends IFFUStringFieldConvertor

  trait IFFDecimalFieldConvertor extends IFFFieldConvertor[IFFDecimal]
  implicit object IFFDecimalField extends IFFDecimalFieldConvertor

  trait IFFZonedDecimalFieldConvertor extends IFFFieldConvertor[IFFZonedDecimal] {
    override protected def convertFromByte(fieldType: IFFZonedDecimal,
                                           fieldBytes: Array[Byte],
                                           decoder: CharsetDecoder): String = {
      val fieldValue = IFFUtils.byteDecode(decoder, fieldBytes, IFFFieldType.NO_LINE_ESC)
      if(StringUtils.isEmpty(fieldValue)) return fieldValue
      fieldType.hostFile match {
        case true =>
          val zonedDecimal: AS400ZonedDecimal = new AS400ZonedDecimal(fieldType.precision, fieldType.scale)
          val dc: DecimalFormat = new DecimalFormat
          dc.setMinimumIntegerDigits(fieldType.precision)
          dc.setMinimumFractionDigits(fieldType.scale)
          dc.setGroupingSize(0)
          dc.setPositivePrefix("")
          dc.setNegativePrefix("-")
          dc.format(zonedDecimal.toDouble(fieldBytes))
        case _ =>
          if (fieldType.scale > 0 && fieldValue.indexOf('.') == -1) {
            val sb = new StringBuilder()
            sb ++= fieldValue.substring(0, fieldValue.length - fieldType.scale)
            sb ++= "."
            sb ++= fieldValue.substring(fieldValue.length - fieldType.scale)
            sb.toString
          } else fieldValue
      }
    }
  }
  implicit object IFFZonedDecimalField extends IFFZonedDecimalFieldConvertor

  trait IFFPackedDecimalFieldConvertor extends IFFFieldConvertor[IFFPackedDecimal] {
    override protected def convertFromByte(fieldType: IFFPackedDecimal,
                                           fieldBytes: Array[Byte],
                                           decoder: CharsetDecoder): String = {
      val sb = new StringBuilder()
      for(index<-0 until fieldBytes.length -1){
        val byte = fieldBytes(index)
        sb ++= String.valueOf((byte & 0xf0)>>>4)
        sb ++= String.valueOf(byte & 0x0f)
      }
      sb ++= String.valueOf((fieldBytes.last & 0xf0) >>> 4)
      val decimalValue = sb.toString
      sb.clear()
      val tsign =
        if ((fieldBytes.last & 0x0f) == 0x0d) "-"
        else ""
      sb ++= tsign
      if(fieldType.scale > 0){
        sb ++= decimalValue.substring(0, decimalValue.length - fieldType.scale)
        sb ++= "."
        sb ++= decimalValue.substring(decimalValue.length - fieldType.scale)
      }else{
        sb ++= decimalValue.substring(decimalValue.length - fieldType.precision, decimalValue.length)
      }
      sb.toString
    }
  }
  implicit object IFFPackedDecimalField extends IFFPackedDecimalFieldConvertor

  trait IFFTrailingDecimalFieldConvertor extends IFFFieldConvertor[IFFTrailingDecimal] {
    override protected def convertFromString(fieldType: IFFTrailingDecimal,
                                             fieldValue: String): String = {
      val sb = new StringBuilder()
      sb ++= fieldValue.substring(fieldValue.length - 1, fieldValue.length).replace(" ", "")
      sb ++= fieldValue.substring(0, fieldValue.length - fieldType.scale - 1)
      sb ++= "."
      sb ++= fieldValue.substring(fieldValue.length - 1 - fieldType.scale, fieldValue.length - 1)
      sb.toString
    }
  }
  implicit object IFFTrailingDecimalField extends IFFTrailingDecimalFieldConvertor

  trait IFFLeadingDecimalFieldConvertor extends IFFFieldConvertor[IFFLeadingDecimal] {
    override protected def convertFromString(fieldType: IFFLeadingDecimal,
                                             fieldValue: String): String = {
      if(StringUtils.isEmpty(fieldValue)) return fieldValue
      if (fieldType.scale > 0 && fieldValue.indexOf('.') == -1) {
        val sb = new StringBuilder
        sb ++= fieldValue.substring(0, fieldValue.length - fieldType.scale).trim
        sb ++= "."
        sb ++= fieldValue.substring(fieldValue.length - fieldType.scale)
        sb.toString
      }else fieldValue
    }
  }
  implicit object IFFLeadingDecimalField extends IFFLeadingDecimalFieldConvertor

  trait IFFBinaryFieldConvertor extends IFFFieldConvertor[IFFBinary] {
    override protected def convertFromByte(fieldType: IFFBinary,
                                           fieldBytes: Array[Byte],
                                           decoder: CharsetDecoder): String = {
      var value: Int = 0
      for(byte<-fieldBytes){
        value = (value << 8) + (byte & 255)
      }
      value.toString
    }
  }
  implicit object IFFBinaryField extends IFFBinaryFieldConvertor

  trait IFFIntegerFieldConvertor extends IFFFieldConvertor[IFFInteger] {
    override protected def convertFromByte(fieldType: IFFInteger,
                                           fieldBytes: Array[Byte],
                                           decoder: CharsetDecoder): String = {
      var value: Int = 0
      for(byte<-fieldBytes){
        value = (value << 8) + (byte & 255)
      }
      value.toString
    }
  }
  implicit object IFFIntegerField extends IFFIntegerFieldConvertor

  trait IFFDoubleFieldConvertor extends IFFFieldConvertor[IFFDouble] {
    override protected def convertFromByte(fieldType: IFFDouble,
                                           fieldBytes: Array[Byte],
                                           decoder: CharsetDecoder): Double = {
      val sign = (fieldBytes.head & 0x80) == 0
      val valueBytes = java.util.Arrays.copyOfRange(fieldBytes, 1, fieldBytes.length)
      val exponent = fieldBytes.head & 0x7f - 64
      val mantissaInt = new BigInteger(valueBytes).longValue()
      val value = mantissaInt * Math.pow(2, 4 * exponent - 56)
      sign match {
        case true => value
        case _=> -value
      }
    }
  }
  implicit object IFFDoubleField extends IFFDoubleFieldConvertor


  trait CDateFieldConvertor extends IFFFieldConvertor[CDate] {
    override def convertFromString(fieldType: CDate,fieldValue: String): String = {
      if (fieldValue.length == 10) {
        fieldValue.replaceAll("0000/00/00|0000-00-00", "          ")
      } else {
        fieldValue.replaceAll("00000000|[\\.]      ", "        ")
      }
    }
  }
  implicit object CDateField extends CDateFieldConvertor

  trait CTimeFieldConvertor extends IFFFieldConvertor[CTime] {
    override def convertFromString(fieldType: CTime,fieldValue: String): String = {
      if (fieldValue.length == 6) {
        "%s:%s:%s.000".format(fieldValue.substring(0, 2),
          fieldValue.substring(2, 4),fieldValue.substring(4, 6))
      } else {
        fieldValue + ".000"
      }
    }
  }
  implicit object CTimeField extends CTimeFieldConvertor

  trait CTimestampFieldConvertor extends IFFFieldConvertor[CTimestamp] {
    override def convertFromString(fieldType: CTimestamp,fieldValue: String): String = {
      if(StringUtils.isNotBlank(fieldValue)){
        "%s %s:%s:%s".format(fieldValue.substring(0, 10),
          fieldValue.substring(10, 12),
          fieldValue.substring(12, 14),
          fieldValue.substring(14, 16))
      }else fieldValue
    }
  }
  implicit object CTimestampField extends CTimestampFieldConvertor

  trait CStringFieldConvertor extends IFFFieldConvertor[CString]
  implicit object CStringField extends CStringFieldConvertor


  trait CDecimalFieldConvertor extends IFFFieldConvertor[CDecimal]
  implicit object CDecimalField extends CDecimalFieldConvertor


  trait CIntegerFieldConvertor extends IFFFieldConvertor[CInteger]
  implicit object CIntegerField extends CIntegerFieldConvertor

  def apply[T<:IFFFieldType](implicit convertor: IFFFieldConvertor[T]) = {
    convertor
  }


}
