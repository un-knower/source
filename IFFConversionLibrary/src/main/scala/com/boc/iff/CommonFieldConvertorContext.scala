package com.boc.iff

import java.nio.charset.CharsetDecoder
import java.text.DecimalFormat

import com.boc.iff.model._

import scala.collection.{JavaConversions, mutable}
import ognl.Ognl
import org.apache.commons.lang.StringUtils


/**
  * Created by cvinc on 2016/6/8.
  *
  * @author www.birdiexx.com
  */
class CommonFieldConvertorContext(val metadata: IFFMetadata, val iffFileInfo: IFFFileInfo, val decoder: CharsetDecoder) extends Serializable {



  private def convert[T <: IFFFieldType](fieldType: T, fieldValue:String)
                                        (implicit convertor: CommonFieldConvertor[T]): String = {
    convertor.convert(fieldType, fieldValue, decoder)
  }

  protected def toObject[T <: IFFFieldType](fieldType:T,fieldValue:String)
                                           (implicit transformer: CommonFieldConvertor[T]):Any = {
    transformer.toObject(fieldType,fieldValue)
  }

  protected def objectToString[T <: IFFFieldType](fieldType:T,fieldValue:Any)
                                                 (implicit transformer: CommonFieldConvertor[T]):String = {
    transformer.objectToString(fieldType,fieldValue)
  }

  def convert(iffField: IFFField, fieldValues: java.util.HashMap[String,Any]): String = {
    val fieldType = iffField.typeInfo
    var fieldValue = ""
    if(StringUtils.isNotEmpty(iffField.expression)){
      val result:Any = iffField.getExpressionValue(fieldValues)
      if(result!=null){
        fieldValues.put(iffField.name,result)
        fieldValue=result.toString
      }
    }else if(fieldValues.containsKey(iffField.name)){
      fieldValue = fieldValues.get(iffField.name).toString
    }

    val newValue = fieldType match {
      case fieldType@IFFDate() => convert(fieldType, fieldValue)
      case fieldType@IFFTime() => convert(fieldType, fieldValue)
      case fieldType@IFFTimestamp() => convert(fieldType, fieldValue)
      case fieldType@IFFString() => convert(fieldType, fieldValue)
      case fieldType@IFFUString() => convert(fieldType, fieldValue)
      case fieldType@IFFDecimal() => convert(fieldType, fieldValue)
      case fieldType@IFFZonedDecimal() => convert(fieldType, fieldValue)
      case fieldType@IFFPackedDecimal() => convert(fieldType, fieldValue)
      case fieldType@IFFTrailingDecimal() => convert(fieldType, fieldValue)
      case fieldType@IFFLeadingDecimal() => convert(fieldType, fieldValue)
      case fieldType@IFFBinary() => convert(fieldType, fieldValue)
      case fieldType@IFFInteger() => convert(fieldType, fieldValue)
      case fieldType@CString() => convert(fieldType, fieldValue)
      case fieldType@CDecimal() =>
        if(StringUtils.isNotEmpty(fieldValue)){
          var pattern = "#"*(fieldType.precision-fieldType.scale)
          if(fieldType.scale>0){
            pattern += "."+"#"*fieldType.scale
          }
          val format = new DecimalFormat(pattern)
          fieldValue = format.format(fieldValue.toDouble)
        }
        convert(fieldType, fieldValue)
      case fieldType@CInteger() =>
        if(StringUtils.isNotEmpty(fieldValue)){
          val pattern = "#"*(fieldType.maxlength)
          val format = new DecimalFormat(pattern)
          fieldValue = format.format(fieldValue.toInt)
        }
        convert(fieldType, fieldValue)
      case fieldType@CDate() => convert(fieldType, fieldValue)
      case fieldType@CTime() => convert(fieldType, fieldValue)
      case fieldType@CTimestamp() => convert(fieldType, fieldValue)
      case _ => convert(IFFString(), fieldValue)
    }
    newValue
  }

  def toObject(iffField:IFFField,fieldValue:String):Any = {
    val newValue = if (StringUtils.isEmpty(fieldValue)) null
    else iffField.typeInfo match {
      case fieldType@IFFDate() => toObject(fieldType, fieldValue)
      case fieldType@IFFTime() => toObject(fieldType, fieldValue)
      case fieldType@IFFTimestamp() => toObject(fieldType, fieldValue)
      case fieldType@IFFString() => toObject(fieldType, fieldValue)
      case fieldType@IFFUString() => toObject(fieldType, fieldValue)
      case fieldType@IFFDecimal() => toObject(fieldType, fieldValue)
      case fieldType@IFFZonedDecimal() => toObject(fieldType, fieldValue)
      case fieldType@IFFPackedDecimal() => toObject(fieldType, fieldValue)
      case fieldType@IFFTrailingDecimal() => toObject(fieldType, fieldValue)
      case fieldType@IFFLeadingDecimal() => toObject(fieldType, fieldValue)
      case fieldType@IFFBinary() => toObject(fieldType, fieldValue)
      case fieldType@IFFInteger() => toObject(fieldType, fieldValue)
      case fieldType@CString() => toObject(fieldType, fieldValue)
      case fieldType@CDecimal() => toObject(fieldType, fieldValue)
      case fieldType@CInteger() => toObject(fieldType, fieldValue)
      case fieldType@CDate() => toObject(fieldType, fieldValue)
      case fieldType@CTime() => toObject(fieldType, fieldValue)
      case fieldType@CTimestamp() => toObject(fieldType, fieldValue)
      case _ => toObject(CString(), fieldValue)
    }
    newValue
  }

  def objectToString(iffField:IFFField,fieldValue:Any):String = {
    val newValue = if(fieldValue==null)"" else iffField.typeInfo match {
      case fieldType@IFFDate() => objectToString(fieldType, fieldValue)
      case fieldType@IFFTime() => objectToString(fieldType, fieldValue)
      case fieldType@IFFTimestamp() => objectToString(fieldType, fieldValue)
      case fieldType@IFFString() => objectToString(fieldType, fieldValue)
      case fieldType@IFFUString() => objectToString(fieldType, fieldValue)
      case fieldType@IFFDecimal() => objectToString(fieldType, fieldValue)
      case fieldType@IFFZonedDecimal() => objectToString(fieldType, fieldValue)
      case fieldType@IFFPackedDecimal() => objectToString(fieldType, fieldValue)
      case fieldType@IFFTrailingDecimal() => objectToString(fieldType, fieldValue)
      case fieldType@IFFLeadingDecimal() => objectToString(fieldType, fieldValue)
      case fieldType@IFFBinary() => objectToString(fieldType, fieldValue)
      case fieldType@IFFInteger() => objectToString(fieldType, fieldValue)
      case fieldType@CString() => objectToString(fieldType,fieldValue)
      case fieldType@CDecimal() => objectToString(fieldType,fieldValue)
      case fieldType@CInteger() => objectToString(fieldType,fieldValue)
      case fieldType@CDate() => objectToString(fieldType,fieldValue)
      case fieldType@CTime() => objectToString(fieldType,fieldValue)
      case fieldType@CTimestamp() => objectToString(fieldType,fieldValue)
      case _ => objectToString(CString(), fieldValue)
    }
    newValue
  }
}

sealed trait CommonFieldWithConvertor {
  protected val commonFieldConvertorContext: CommonFieldConvertorContext = null
  protected val iffField: IFFField = null

  def convert(fieldValue: java.util.HashMap[String,Any]): String = {
    commonFieldConvertorContext.convert(iffField, fieldValue)
  }

  def objectToString(fieldValue:Any):String = {
    commonFieldConvertorContext.objectToString(iffField,fieldValue).toString
  }

  def toObject(fieldValue:String):Any = {
    commonFieldConvertorContext.toObject(iffField,fieldValue)
  }

}

/**
  * @author www.birdiexx.com
  */
object CommonFieldConvertorContext  {
  implicit def commonFieldWithConvertor(field: IFFField)
                                    (implicit context: CommonFieldConvertorContext): CommonFieldWithConvertor =
    new CommonFieldWithConvertor() {
      override  protected val commonFieldConvertorContext: CommonFieldConvertorContext = context
      override  protected val iffField: IFFField = field
    }
}
