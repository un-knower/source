package com.boc.iff

import java.math.BigInteger
import java.nio.charset.CharsetDecoder
import java.text.DecimalFormat

import com.boc.iff.model._
import com.ibm.as400.access.AS400ZonedDecimal
import org.apache.commons.lang3.StringUtils

/**
  * Created by cvinc on 2016/6/23.
  * @author www.birdiexx.com
  */

@annotation.implicitNotFound(msg = "No implicit IFFFieldConvertor defined for ${T}.")
sealed trait CommonFieldConvertor[T<:IFFFieldType] {
  protected def convertFromString(fieldType: T, fieldValue: String): Any = fieldValue
  def convert(fieldType: T, fieldValue: String, decoder: CharsetDecoder): String = {
    val fieldValueConvertFromString = convertFromString(fieldType,fieldValue)

    val formattedFieldValue = fieldType match {
      case fieldType: IFFFormatable if fieldType.formatSpec != null =>
        fieldType.formatSpec.getFormatObj.format(fieldValueConvertFromString)
      case _ => fieldValueConvertFromString.toString
    }

    val trimedFieldValue = fieldType match {
      case fieldType: IFFNeedTrim if StringUtils.isNotEmpty(formattedFieldValue) =>
        formattedFieldValue.trim
      case _ => formattedFieldValue
    }
    trimedFieldValue.toString
  }
}

object CommonFieldConvertor {

  trait IFFDateFieldConvertor extends CommonFieldConvertor[IFFDate] {

  }
  implicit object IFFDateField extends IFFDateFieldConvertor

  trait IFFTimeFieldConvertor extends CommonFieldConvertor[IFFTime] {

  }
  implicit object IFFTimeField extends IFFTimeFieldConvertor

  trait IFFTimestampFieldConvertor extends CommonFieldConvertor[IFFTimestamp] {

  }

  implicit object IFFTimestampField extends IFFTimestampFieldConvertor

  trait IFFStringFieldConvertor extends CommonFieldConvertor[IFFString]
  implicit object IFFStringField extends IFFStringFieldConvertor

  trait IFFUStringFieldConvertor extends CommonFieldConvertor[IFFUString]
  implicit object IFFUStringField extends IFFUStringFieldConvertor

  trait IFFDecimalFieldConvertor extends CommonFieldConvertor[IFFDecimal]
  implicit object IFFDecimalField extends IFFDecimalFieldConvertor

  trait IFFZonedDecimalFieldConvertor extends CommonFieldConvertor[IFFZonedDecimal] {
  }
  implicit object IFFZonedDecimalField extends IFFZonedDecimalFieldConvertor

  trait IFFPackedDecimalFieldConvertor extends CommonFieldConvertor[IFFPackedDecimal] {

  }
  implicit object IFFPackedDecimalField extends IFFPackedDecimalFieldConvertor

  trait IFFTrailingDecimalFieldConvertor extends CommonFieldConvertor[IFFTrailingDecimal] {

  }
  implicit object IFFTrailingDecimalField extends IFFTrailingDecimalFieldConvertor

  trait IFFLeadingDecimalFieldConvertor extends CommonFieldConvertor[IFFLeadingDecimal] {

  }
  implicit object IFFLeadingDecimalField extends IFFLeadingDecimalFieldConvertor

  trait IFFBinaryFieldConvertor extends CommonFieldConvertor[IFFBinary] {

  }
  implicit object IFFBinaryField extends IFFBinaryFieldConvertor

  trait IFFIntegerFieldConvertor extends CommonFieldConvertor[IFFInteger] {

  }
  implicit object IFFIntegerField extends IFFIntegerFieldConvertor

  trait IFFDoubleFieldConvertor extends CommonFieldConvertor[IFFDouble] {

  }
  implicit object IFFDoubleField extends IFFDoubleFieldConvertor


  trait CDateFieldConvertor extends CommonFieldConvertor[CDate] {

  }
  implicit object CDateField extends CDateFieldConvertor

  trait CTimeFieldConvertor extends CommonFieldConvertor[CTime] {

  }
  implicit object CTimeField extends CTimeFieldConvertor

  trait CTimestampFieldConvertor extends CommonFieldConvertor[CTimestamp] {

  }
  implicit object CTimestampField extends CTimestampFieldConvertor

  trait CStringFieldConvertor extends CommonFieldConvertor[CString]
  implicit object CStringField extends CStringFieldConvertor


  trait CDecimalFieldConvertor extends CommonFieldConvertor[CDecimal]
  implicit object CDecimalField extends CDecimalFieldConvertor


  trait CIntegerFieldConvertor extends CommonFieldConvertor[CInteger]
  implicit object CIntegerField extends CIntegerFieldConvertor

  def apply[T<:IFFFieldType](implicit convertor: IFFFieldConvertor[T]) = {
    convertor
  }

  /**
    * @author www.birdiexx.com
    */
}
