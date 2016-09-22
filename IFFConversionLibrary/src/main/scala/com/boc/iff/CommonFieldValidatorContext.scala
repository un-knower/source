package com.boc.iff

import com.boc.iff.model._

import scala.collection.mutable.HashMap

/**
  * Created by cvinc on 2016/6/8.
  */
class CommonFieldValidatorContext() extends Serializable {

  private def validate[T <: IFFFieldType](fieldType: T, fieldValue: String)
                                        (implicit validator: CommonFieldValidator[T]) = {
    validator.validate(fieldType, fieldValue)
  }

  def validateField(iffField: IFFField, fieldValues: HashMap[String,Any]) = {
    val fieldType = iffField.typeInfo
    val fieldValue = fieldValues.getOrElse(iffField.name, "").toString
    val normalCheck = fieldType match {
      case fieldType@IFFDate() => validate(fieldType, fieldValue)
      case fieldType@IFFTime() => validate(fieldType, fieldValue)
      case fieldType@IFFTimestamp() => validate(fieldType, fieldValue)
      case fieldType@IFFString() => validate(fieldType, fieldValue)
      case fieldType@IFFUString() => validate(fieldType, fieldValue)
      case fieldType@IFFDecimal() => validate(fieldType, fieldValue)
      case fieldType@IFFZonedDecimal() => validate(fieldType, fieldValue)
      case fieldType@IFFPackedDecimal() => validate(fieldType, fieldValue)
      case fieldType@IFFTrailingDecimal() => validate(fieldType, fieldValue)
      case fieldType@IFFLeadingDecimal() => validate(fieldType, fieldValue)
      case fieldType@IFFBinary() => validate(fieldType, fieldValue)
      case fieldType@IFFInteger() => validate(fieldType, fieldValue)
      case fieldType@IFFDouble() => validate(fieldType, fieldValue)
      case fieldType@CString() => validate(fieldType, fieldValue)
      case fieldType@CDecimal() => validate(fieldType, fieldValue)
      case fieldType@CInteger() => validate(fieldType, fieldValue)
      case fieldType@CDate() => validate(fieldType, fieldValue)
      case fieldType@CTime() => validate(fieldType, fieldValue)
      case fieldType@CTimestamp() => validate(fieldType, fieldValue)
      case _ =>validate(CString(), fieldValue)
    }
    val expressionCheck = FieldValidator.validatExpression(iffField.validators,fieldValues)
    expressionCheck && normalCheck
  }
}

sealed trait CommonFieldWithValidator {
  protected val commonFieldValidatorContext: CommonFieldValidatorContext = null
  protected val iffField: IFFField = null

  def validateField(fieldValue:HashMap[String,Any]) = {
    commonFieldValidatorContext.validateField(iffField, fieldValue)
  }

}

object CommonFieldValidatorContext {
  implicit def CommonFieldWithValidator(field: IFFField)
                                    (implicit context: CommonFieldValidatorContext): CommonFieldWithValidator =
    new CommonFieldWithValidator() {
      override protected val commonFieldValidatorContext: CommonFieldValidatorContext = context
      override protected val iffField: IFFField = field
    }
}