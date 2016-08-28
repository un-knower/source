package com.boc.iff

import java.nio.charset.CharsetDecoder

import com.boc.iff.model._

/**
  * Created by cvinc on 2016/6/8.
  */
class IFFFieldConvertorContext(val metadata: IFFMetadata, val iffFileInfo: IFFFileInfo, val decoder: CharsetDecoder) extends Serializable {

  private def convert[T <: IFFFieldType](fieldType: T, fieldBytes: Array[Byte])
                                        (implicit convertor: IFFFieldConvertor[T]): String = {
    convertor.convert(fieldType, fieldBytes, decoder)
  }

  def convert(iffField: IFFField, fieldBytes: Array[Byte]): String = {
    val fieldType = iffField.typeInfo
    fieldType match {
      case fieldType@IFFDate() => convert(fieldType, fieldBytes)
      case fieldType@IFFTime() => convert(fieldType, fieldBytes)
      case fieldType@IFFTimestamp() => convert(fieldType, fieldBytes)
      case fieldType@IFFString() => convert(fieldType, fieldBytes)
      case fieldType@IFFUString() => convert(fieldType, fieldBytes)
      case fieldType@IFFDecimal() => convert(fieldType, fieldBytes)
      case fieldType@IFFZonedDecimal() => convert(fieldType, fieldBytes)
      case fieldType@IFFPackedDecimal() => convert(fieldType, fieldBytes)
      case fieldType@IFFTrailingDecimal() => convert(fieldType, fieldBytes)
      case fieldType@IFFLeadingDecimal() => convert(fieldType, fieldBytes)
      case fieldType@IFFBinary() => convert(fieldType, fieldBytes)
      case fieldType@IFFInteger() => convert(fieldType, fieldBytes)
      case fieldType@CString() => convert(fieldType, fieldBytes)
      case fieldType@CDecimal() => convert(fieldType, fieldBytes)
      case fieldType@CInteger() => convert(fieldType, fieldBytes)
      case fieldType@CDate() => convert(fieldType, fieldBytes)
      case fieldType@CTime() => convert(fieldType, fieldBytes)
      case fieldType@CTimestamp() => convert(fieldType, fieldBytes)
      case _ => convert(IFFString(), fieldBytes)
    }
  }
}

sealed trait IFFFieldWithConvertor {
  protected val iffFieldConvertorContext: IFFFieldConvertorContext = null
  protected val iffField: IFFField = null

  def convert(fieldBytes: Array[Byte]): String = {
    iffFieldConvertorContext.convert(iffField, fieldBytes)
  }

}

object IFFFieldConvertorContext {
  implicit def iffFieldWithConvertor(field: IFFField)
                                    (implicit context: IFFFieldConvertorContext): IFFFieldWithConvertor =
    new IFFFieldWithConvertor() {
      override protected val iffFieldConvertorContext: IFFFieldConvertorContext = context
      override protected val iffField: IFFField = field
    }
}