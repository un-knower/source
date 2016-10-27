package com.boc.iff

import java.nio.charset.CharsetDecoder
import java.text.DecimalFormat

import com.boc.iff.model._

import scala.collection.{JavaConversions, mutable}
import ognl.Ognl
import org.apache.commons.lang.StringUtils


/**
  * Created by cvinc on 2016/6/8.
  * @author www.birdiexx.com
  */
class CommonFieldConvertorContext(val metadata: IFFMetadata, val iffFileInfo: IFFFileInfo, val decoder: CharsetDecoder) extends Serializable {

  private def convert[T <: IFFFieldType](fieldType: T, fieldValue:String)
                                        (implicit convertor: CommonFieldConvertor[T]): String = {
    convertor.convert(fieldType, fieldValue, decoder)
  }

  def convert(iffField: IFFField, fieldValues: mutable.HashMap[String,Any]): String = {
    val fieldType = iffField.typeInfo
    var fieldValue = ""
    if(StringUtils.isNotEmpty(iffField.expression)){
      val result = Ognl.getValue(iffField.expression,JavaConversions.mapAsJavaMap(fieldValues))
      if(result!=null){
        fieldValues += (iffField.name->result)
        fieldValue=result.toString
      }
    }else if(fieldValues.contains(iffField.name)){
      fieldValue = fieldValues(iffField.name).toString
    }
    fieldType match {
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
  }
}

sealed trait CommonFieldWithConvertor {
  protected val commonFieldConvertorContext: CommonFieldConvertorContext = null
  protected val iffField: IFFField = null

  def convert(fieldValue: mutable.HashMap[String,Any]): String = {
    commonFieldConvertorContext.convert(iffField, fieldValue)
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
