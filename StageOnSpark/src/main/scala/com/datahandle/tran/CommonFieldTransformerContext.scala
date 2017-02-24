package com.datahandle.tran

import com.boc.iff.model.{CDate, CDecimal, CInteger, CString, CTime, CTimestamp, IFFBinary, IFFDate, IFFDecimal, IFFDouble, IFFField, IFFFieldType, IFFInteger, IFFLeadingDecimal, IFFPackedDecimal, IFFString, IFFTime, IFFTimestamp, IFFTrailingDecimal, IFFUString, IFFZonedDecimal}

/**
  * Created by scutlxj on 2017/2/22.
  */
class CommonFieldTransformerContext extends Serializable {

  protected def executeFun[T <: IFFFieldType](fieldType: T,fun:String,fieldValue:String,params:String*)
                                (implicit transformer: CommonFieldTransformer[T]):Any = {
    fun match {
      case "to_char" => transformer.toChar (fieldType, fieldValue)
      case "to_object" => transformer.toObject (fieldType, fieldValue)
      case _ => fieldValue
    }
  }

  def transform(iffField: IFFField,fun:String,fieldValue:String,params:String*):Any = {
    val newValue = iffField.typeInfo match {
      case fieldType@CString() => executeFun(fieldType,fieldValue,fun,params:_*)
      case fieldType@CDecimal() => executeFun(fieldType,fieldValue,fun,params:_*)
      case fieldType@CInteger() => executeFun(fieldType,fieldValue,fun,params:_*)
      case fieldType@CDate() => executeFun(fieldType,fieldValue,fun,params:_*)
      case fieldType@CTime() => executeFun(fieldType,fieldValue,fun,params:_*)
      case fieldType@CTimestamp() => executeFun(fieldType,fieldValue,fun,params:_*)
      case _ => fieldValue
    }
    newValue
  }
}

sealed trait CommonFieldWithTransformer {
  protected val commonFieldTransformerContext: CommonFieldTransformerContext = null
  protected val iffField: IFFField = null

  def to_char(fieldValue:String,pattern:String):String = {
    commonFieldTransformerContext.transform(iffField,"to_char",fieldValue,pattern).toString
  }

  def toObject(fieldValue:String):Any = {
    commonFieldTransformerContext.transform(iffField,"to_object",fieldValue).toString
  }

}

object CommonFieldTransformerContext {
  implicit def CommonFieldWithValidator(field: IFFField)
                                       (implicit context: CommonFieldTransformerContext): CommonFieldWithTransformer =
    new CommonFieldWithTransformer() {
      override protected val commonFieldTransformerContext: CommonFieldTransformerContext = context
      override protected val iffField: IFFField = field
    }

  /**
    * @author www.birdiexx.com
    */
}
