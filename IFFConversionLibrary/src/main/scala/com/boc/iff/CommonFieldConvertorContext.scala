package com.boc.iff

import java.nio.charset.CharsetDecoder

import com.boc.iff.model._

/**
  * Created by cvinc on 2016/6/8.
  */
class CommonFieldConvertorContext(val metadata: IFFMetadata, val iffFileInfo: IFFFileInfo, val decoder: CharsetDecoder) extends Serializable {

  private def convert[T <: IFFFieldType](fieldType: T, fieldValue:String)
                                        (implicit convertor: CommonFieldConvertor[T]): String = {
    convertor.convert(fieldType, fieldValue, decoder)
  }

  def convert(iffField: IFFField, fieldValue: String): String = {
    val fieldType = iffField.typeInfo
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
      case fieldType@CDecimal() => convert(fieldType, fieldValue)
      case fieldType@CInteger() => convert(fieldType, fieldValue)
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

  def convert(fieldValue: String): String = {
    commonFieldConvertorContext.convert(iffField, fieldValue)
  }

}

object CommonFieldConvertorContext  {
  implicit def commonFieldWithConvertor(field: IFFField)
                                    (implicit context: CommonFieldConvertorContext): CommonFieldWithConvertor =
    new CommonFieldWithConvertor() {
      override  protected val commonFieldConvertorContext: CommonFieldConvertorContext = context
      override  protected val iffField: IFFField = field
    }
}