package com.datahandle.tran

import java.text.SimpleDateFormat

import com.boc.iff.model._
import org.apache.commons.lang3.StringUtils

/**
  * Created by scutlxj on 2017/2/22.
  */
@annotation.implicitNotFound(msg = "No implicit CommonFieldTransformer defined for ${T}.")
sealed trait CommonFieldTransformer[T<:IFFFieldType]  {

  def toChar(fieldType: T, fieldValue: String):String={fieldValue}

  def toObject(fieldType: T, fieldValue: String):Any={fieldValue}

}

object CommonFieldTransformer {
  trait CStringFieldTransformer extends CommonFieldTransformer[CString] {
    override def toChar(fieldType: CString,fieldValue: String) = {
      "CString"
    }

  }
  implicit object CStringTransformField extends CStringFieldTransformer



  trait CDecimalFieldTransformer extends CommonFieldTransformer[CDecimal] {

    override def toChar(fieldType: CDecimal,fieldValue: String) = {
      "CDecimal"
    }

    override def toObject(fieldType: CDecimal, fieldValue: String):Any={fieldValue.toDouble}
  }
  implicit object CDecimalTransformField extends CDecimalFieldTransformer

  trait CIntegerFieldTransformer extends CommonFieldTransformer[CInteger] {
    override def toChar(fieldType: CInteger, fieldValue: String) = {
      "CInteger"
    }

    override def toObject(fieldType: CInteger, fieldValue: String):Any={fieldValue.toInt}
  }
  implicit object CIntegerTransformField extends CIntegerFieldTransformer

  trait CDateFieldTransformer extends CommonFieldTransformer[CDate] {
    override def toChar(fieldType: CDate,fieldValue: String) = {
      "CDate"
    }

    override def toObject(fieldType: CDate, fieldValue: String):Any={
      if(fieldType.formatSpec!=null){
        fieldType.formatSpec.getFormatObj.format(fieldValue)
      }else{
        val pattern = if(StringUtils.isNotEmpty(fieldType.pattern))fieldType.pattern else "yyyyMMdd"
        val format = new SimpleDateFormat(pattern)
        format.parse(fieldValue)
      }
    }
  }
  implicit object CDateTransformField extends CDateFieldTransformer

  trait CTimeFieldTransformer extends CommonFieldTransformer[CTime] {
    override def toChar(fieldType: CTime,fieldValue: String) = {
      "CTime"
    }
  }
  implicit object CTimeTransformField extends CTimeFieldTransformer


  trait CTimestampFieldTransformer extends CommonFieldTransformer[CTimestamp] {
    override def toChar(fieldType: CTimestamp,fieldValue: String) = {
      "CTimestamp"

    }
  }
  implicit object CTimestampTransformField extends CTimestampFieldTransformer

  trait IFFDateFieldTransformer extends CommonFieldTransformer[IFFDate]
  implicit object IFFDateTransformField extends IFFDateFieldTransformer



  trait IFFTimeFieldTransformer extends CommonFieldTransformer[IFFTime]
  implicit object IFFTimeTransformField extends IFFTimeFieldTransformer


  trait IFFTimestampFieldTransformer extends CommonFieldTransformer[IFFTimestamp]
  implicit object IFFTimestampTransformField extends IFFTimestampFieldTransformer


  trait IFFStringFieldTransformer extends CommonFieldTransformer[IFFString]
  implicit object IFFStringTransformField extends IFFStringFieldTransformer


  trait IFFUStringFieldTransformer extends CommonFieldTransformer[IFFUString]
  implicit object IFFUStringTransformField extends IFFUStringFieldTransformer


  trait IFFDecimalFieldTransformer extends CommonFieldTransformer[IFFDecimal]
  implicit object IFFDecimalTransformField extends IFFDecimalFieldTransformer


  trait IFFZonedDecimalFieldTransformer extends CommonFieldTransformer[IFFZonedDecimal]
  implicit object IFFZonedDecimalTransformField extends IFFZonedDecimalFieldTransformer


  trait IFFPackedDecimalFieldTransformer extends CommonFieldTransformer[IFFPackedDecimal]
  implicit object IFFPackedDecimalTransformField extends IFFPackedDecimalFieldTransformer


  trait IFFTrailingDecimalFieldTransformer extends CommonFieldTransformer[IFFTrailingDecimal]
  implicit object IFFTrailingDecimalTransformField extends IFFTrailingDecimalFieldTransformer


  trait IFFLeadingDecimalFieldTransformer extends CommonFieldTransformer[IFFLeadingDecimal]
  implicit object IFFLeadingDecimalTransformField extends IFFLeadingDecimalFieldTransformer

  trait IFFBinaryFieldTransformer extends CommonFieldTransformer[IFFBinary]
  implicit object IFFBinaryTransformField extends IFFBinaryFieldTransformer


  trait IFFIntegerFieldTransformer extends CommonFieldTransformer[IFFInteger]
  implicit object IFFIntegerTransformField extends IFFIntegerFieldTransformer


  trait IFFDoubleFieldTransformer extends CommonFieldTransformer[IFFDouble]
  implicit object IIFFDoubleTransformField extends IFFDoubleFieldTransformer


  def apply[T<:IFFFieldType](implicit transformer: CommonFieldTransformer[T]) = {
    transformer
  }

  /**
    * @author www.birdiexx.com
    */
}
