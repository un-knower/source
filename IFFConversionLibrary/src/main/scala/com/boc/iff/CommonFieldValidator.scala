package com.boc.iff
import java.text.DecimalFormat

import com.boc.iff.model._
import org.apache.commons.lang.StringUtils
/**
  * Created by cvinc on 2016/6/23.
  *
  * @author www.birdiexx.com
  */


@annotation.implicitNotFound(msg = "No implicit CommonFieldValidator defined for ${T}.")
sealed trait CommonFieldValidator[T<:IFFFieldType]  {

  def validate(fieldType: T, fieldValue: String):Boolean={true}
}

object CommonFieldValidator {
  trait CStringFieldValidator extends CommonFieldValidator[CString] {
    //校验长度
    override def validate(fieldType: CString,fieldValue: String) = {
      FieldValidator.validatBase(fieldType,fieldValue)
    }
  }
  implicit object CStringValidField extends CStringFieldValidator



  trait CDecimalFieldValidator extends CommonFieldValidator[CDecimal] {
    //检验长度、数字、格式

    override def validate(fieldType: CDecimal,fieldValue: String) = {
      var checkValue = fieldValue
      if (StringUtils.isNotEmpty(checkValue)) {
        var pattern = "#" * (fieldType.precision - fieldType.scale)
        if (fieldType.scale > 0) {
          pattern += "." + "#" * fieldType.scale
        }
        val format = new DecimalFormat(pattern)
        checkValue = format.format(checkValue.toDouble)
        if (checkValue.toDouble != fieldValue.toDouble) {
          false
        }else{
          FieldValidator.validatCDecimal(fieldType, checkValue)
        }
      }else{
        FieldValidator.validatCDecimal(fieldType, checkValue)
      }

    }
  }
  implicit object CDecimalValidField extends CDecimalFieldValidator

  trait CIntegerFieldValidator extends CommonFieldValidator[CInteger] {
    //检验数字
    override def validate(fieldType: CInteger,fieldValue: String) = {
      var checkVal = fieldValue
      if(StringUtils.isNotEmpty(checkVal)&&fieldType.maxlength>0){
        val pattern = "#"*(fieldType.maxlength)
        val format = new DecimalFormat(pattern)
        checkVal = format.format(checkVal.toInt)
      }
      FieldValidator.validatCInt(fieldType,checkVal)
    }
  }
  implicit object CIntegerValidField extends CIntegerFieldValidator

  trait CDateFieldValidator extends CommonFieldValidator[CDate] {
    //检验数字和日期
    override def validate(fieldType: CDate,fieldValue: String) = {

      FieldValidator.validatCDate(fieldType,fieldValue)

    }
  }
  implicit object CDateValidField extends CDateFieldValidator

  trait CTimeFieldValidator extends CommonFieldValidator[CTime] {
    //检验时间
    override def validate(fieldType: CTime,fieldValue: String) = {

      FieldValidator.validatCTime(fieldType,fieldValue)
    }
  }
  implicit object CTimeValidField extends CTimeFieldValidator

  trait CTimestampFieldValidator extends CommonFieldValidator[CTimestamp] {
    //检验时间
    override def validate(fieldType: CTimestamp,fieldValue: String) = {

      FieldValidator.validatCTimestamp(fieldType,fieldValue)

    }
  }
  implicit object CTimestampValidField extends CTimestampFieldValidator


  trait IFFDateFieldValidator extends CommonFieldValidator[IFFDate]
  implicit object IFFDateValidField extends IFFDateFieldValidator



  trait IFFTimeFieldValidator extends CommonFieldValidator[IFFTime]
  implicit object IFFTimeValidField extends IFFTimeFieldValidator


  trait IFFTimestampFieldValidator extends CommonFieldValidator[IFFTimestamp]
  implicit object IFFTimestampValidField extends IFFTimestampFieldValidator


  trait IFFStringFieldValidator extends CommonFieldValidator[IFFString]
  implicit object IFFStringValidField extends IFFStringFieldValidator


  trait IFFUStringFieldValidator extends CommonFieldValidator[IFFUString]
  implicit object IFFUStringValidField extends IFFUStringFieldValidator


  trait IFFDecimalFieldValidator extends CommonFieldValidator[IFFDecimal]
  implicit object IFFDecimalValidField extends IFFDecimalFieldValidator


  trait IFFZonedDecimalFieldValidator extends CommonFieldValidator[IFFZonedDecimal]
  implicit object IFFZonedDecimalValidField extends IFFZonedDecimalFieldValidator


  trait IFFPackedDecimalFieldValidator extends CommonFieldValidator[IFFPackedDecimal]
  implicit object IFFPackedDecimalValidField extends IFFPackedDecimalFieldValidator


  trait IFFTrailingDecimalFieldValidator extends CommonFieldValidator[IFFTrailingDecimal]
  implicit object IFFTrailingDecimalValidField extends IFFTrailingDecimalFieldValidator


  trait IFFLeadingDecimalFieldValidator extends CommonFieldValidator[IFFLeadingDecimal]
  implicit object IFFLeadingDecimalValidField extends IFFLeadingDecimalFieldValidator

  trait IFFBinaryFieldValidator extends CommonFieldValidator[IFFBinary]
  implicit object IFFBinaryValidField extends IFFBinaryFieldValidator


  trait IFFIntegerFieldValidator extends CommonFieldValidator[IFFInteger]
  implicit object IFFIntegerValidField extends IFFIntegerFieldValidator


  trait IFFDoubleFieldValidator extends CommonFieldValidator[IFFDouble]
  implicit object IIFFDoubleValidField extends IFFDoubleFieldValidator


  def apply[T<:IFFFieldType](implicit validator: CommonFieldValidator[T]) = {
    validator
  }

  /**
    * @author www.birdiexx.com
    */
}
