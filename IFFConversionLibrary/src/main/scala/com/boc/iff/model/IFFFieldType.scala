package com.boc.iff.model

import java.util.StringTokenizer

import com.boc.iff.FormatSpec
import org.apache.commons.lang3.StringUtils

/**
  * Created by cvinc on 2016/6/9.
  */
sealed trait IFFFieldType{
  var required =false
}
sealed trait IFFNeedTrim
sealed trait IFFByteDecodeWithLineEsc
sealed trait IFFByteDecodeWithOutLineEsc
sealed trait IFFFormatable {
  var formatSpec: FormatSpec = null
}
sealed trait IFFDecimalType{
  var precision: Int = 0
  var scale: Int = 0
}
sealed trait IFFMaxlengthType{
  var chartSet: String = ""
  var maxlength: Int = 0
}

sealed trait IFFDateTimeType{
  var pattern:String = "yyyyMMdd"
}
sealed trait IFFHostFile{
  var hostFile: Boolean = false
}

case class IFFDate() extends IFFFieldType with IFFByteDecodeWithOutLineEsc
case class IFFTime() extends IFFFieldType with IFFByteDecodeWithOutLineEsc
case class IFFTimestamp() extends IFFFieldType with IFFByteDecodeWithOutLineEsc
case class IFFString() extends IFFFieldType with IFFByteDecodeWithLineEsc with IFFFormatable with IFFNeedTrim
case class IFFUString() extends IFFFieldType with IFFByteDecodeWithLineEsc with IFFFormatable with IFFNeedTrim
case class IFFDecimal() extends IFFFieldType with IFFByteDecodeWithLineEsc with IFFDecimalType with IFFFormatable with IFFNeedTrim
case class IFFZonedDecimal() extends IFFFieldType with IFFDecimalType with IFFFormatable with IFFHostFile
case class IFFPackedDecimal() extends IFFFieldType with IFFDecimalType with IFFFormatable
case class IFFTrailingDecimal() extends IFFFieldType with IFFByteDecodeWithOutLineEsc with IFFDecimalType
case class IFFLeadingDecimal() extends IFFFieldType with IFFByteDecodeWithOutLineEsc with IFFDecimalType with IFFFormatable
case class IFFBinary() extends IFFFieldType
case class IFFInteger() extends IFFFieldType
case class IFFDouble() extends IFFFieldType with IFFFormatable
case class CString() extends IFFFieldType with IFFByteDecodeWithLineEsc with IFFFormatable with IFFNeedTrim with IFFMaxlengthType
case class CDecimal() extends IFFFieldType with IFFByteDecodeWithLineEsc with IFFDecimalType with IFFFormatable
case class CInteger() extends IFFFieldType with IFFByteDecodeWithLineEsc with IFFMaxlengthType
case class CDate() extends IFFFieldType with IFFByteDecodeWithOutLineEsc with IFFDateTimeType
case class CTime() extends IFFFieldType with IFFByteDecodeWithOutLineEsc with IFFDateTimeType
case class CTimestamp() extends IFFFieldType with IFFByteDecodeWithOutLineEsc with IFFDateTimeType


object IFFFieldType {

  val LINE_ESC = true
  val NO_LINE_ESC = false

  object FieldType extends Enumeration {
    type ValueType = Value
    val STRING = Value("string")
    val USTRING = Value("ustring")
    val DECIMAL = Value("decimal")
    val ZONED_DECIMAL = Value("zoned decimal")
    val PACKED_DECIMAL = Value("packed decimal")
    val BINARY = Value("int16")
    val INTEGER = Value("int32")
    val DOUBLE = Value("double")
    val DATE = Value("date")
    val TIME = Value("time")
    val TIMESTAMP = Value("timestamp")
    val TRAILING_DECIMAL = Value("trailing decimal")
    val LEADING_DECIMAL = Value("leading decimal")
    val CSTRING = Value("cstring")
    val CDECIMAL = Value("cdecimal")
    val CINTEGER = Value("cint32")
    val CDATE = Value("cdate")
    val CTIME = Value("ctime")
    val CTIMESTAMP = Value("ctimestamp")
  }

  def getFieldType(metadata: IFFMetadata, iffFileInfo: IFFFileInfo, iffField: IFFField): IFFFieldType = {
    val stringTokenizer = new StringTokenizer(iffField.`type`, "[,]")
    val fieldTypeName = stringTokenizer.nextToken()
    val fixedFieldTypeName =
      if(StringUtils.isNotEmpty(fieldTypeName)) fieldTypeName.trim.toLowerCase
      else "string"
    val fieldTypeValue: FieldType.ValueType =
      try{
        FieldType.withName(fixedFieldTypeName)
      }catch{
        case e: NoSuchElementException =>
          if(fixedFieldTypeName.endsWith("packed decimal")) FieldType.PACKED_DECIMAL
          else FieldType.STRING
          //throw new Exception("Unsupported column type: " + iffField.getName + ":" + iffField.`type`)
      }
    val fieldType = fieldTypeValue match {
      case FieldType.STRING => IFFString()
      case FieldType.USTRING => IFFUString()
      case FieldType.DECIMAL => IFFDecimal()
      case FieldType.ZONED_DECIMAL => IFFZonedDecimal()
      case FieldType.PACKED_DECIMAL => IFFPackedDecimal()
      case FieldType.BINARY => IFFBinary()
      case FieldType.INTEGER => IFFInteger()
      case FieldType.DOUBLE => IFFDouble()
      case FieldType.DATE => IFFDate()
      case FieldType.TIME => IFFTime()
      case FieldType.TIMESTAMP => IFFTimestamp()
      case FieldType.TRAILING_DECIMAL => IFFTrailingDecimal()
      case FieldType.LEADING_DECIMAL => IFFLeadingDecimal()
      case FieldType.CSTRING => CString()
      case FieldType.CDECIMAL => CDecimal()
      case FieldType.CINTEGER => CInteger()
      case FieldType.CDATE => CDate()
      case FieldType.CTIME => CTime()
      case FieldType.CTIMESTAMP => CTimestamp()

    }
    fieldType.required = iffField.required
    fieldType match {
      case fieldType: IFFFormatable => fieldType.formatSpec = iffField.formatSpec
      case _ =>
    }
    fieldType match {
      case fieldType: IFFDecimalType =>
        fieldType.precision = stringTokenizer.nextToken().toInt
        fieldType.scale = stringTokenizer.hasMoreTokens match {
          case true => stringTokenizer.nextToken().toInt
          case _ => 0
        }
      case _ =>
    }
    fieldType match {
      case fieldType: IFFDateTimeType =>
        fieldType.pattern = stringTokenizer.hasMoreTokens match{
          case true => stringTokenizer.nextToken
          case _ =>  "yyyyMMdd"
        }
      case _ =>
    }

    fieldType match {
      case fieldType: IFFMaxlengthType =>
        fieldType.maxlength = stringTokenizer.hasMoreTokens match {
          case true => stringTokenizer.nextToken().toInt
          case _ => 0
        }
        fieldType.chartSet = metadata.sourceCharset
      case _ =>
    }
    fieldType match {
      case fieldType: IFFHostFile => fieldType.hostFile = iffFileInfo.isHost
      case _ =>
    }
    fieldType
  }
}