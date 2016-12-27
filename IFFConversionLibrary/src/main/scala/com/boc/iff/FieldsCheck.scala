package com.boc.iff

import java.io.FileInputStream
import java.text.SimpleDateFormat
import java.util.Properties

import com.boc.iff.model.{CDate, CDecimal, CInteger, CTime, CTimestamp, IFFDecimalType, IFFFieldType, IFFMaxlengthType}
import ognl.Ognl
import org.apache.commons.lang.StringUtils

import scala.collection.{JavaConversions, mutable}
/**
  * Created by birdie on 8/25/16.
  */
trait FieldsCheck {

/**
  val logger = new ECCLogger()
  val prop = new Properties()
  prop.load(new FileInputStream("/app/birdie/bochk/IFFConversion/config/config.properties"))
  logger.configure(prop)*/





  /**
    * check if the field length less than maxlength
    *
    * @param value
    * @param maxlength
    * @param charEncoding
    */
  protected def checkMaxlength(value:String,maxlength:Int,charEncoding:String):Boolean={
    if(StringUtils.isEmpty(value)){
      true
    }else{
      value.getBytes(charEncoding).length <= maxlength
    }
  }



  /**
    * check the value if is long type
    *
    * @param value
    * */
  protected def checkLong(value:String):Boolean={
    if(StringUtils.isEmpty(value)){
      true
    }else {
      try {
        java.lang.Long.parseLong(value)
        true
      } catch {
        case e: Exception => false
      }
    }
  }

  /**
    * check the value if is int type
    *
    * @param value
    */
  protected def checkInt(value:String):Boolean={
    if(StringUtils.isEmpty(value)){
      true
    }else {
      try {
        java.lang.Integer.parseInt(value)
        true
      } catch {
        case e: Exception => false
      }
    }
  }


  /**
    *
    * @param value
    * @param pattern   */
  protected def checkDate(value:String,pattern:String):Boolean={
    if(StringUtils.isEmpty(value)){
      true
    }else {
      val format: SimpleDateFormat = new SimpleDateFormat(pattern)
      try {
        value.equals(format.format(format.parse(value)))
      } catch {
        case e: Exception => false
      }
    }
  }


  /**
    *check the value if fix Decimal
    *
    *  @param value
    * @param len1
    * @param len2
    */
  protected def checkDecimal(value:String,len1:Int,len2:Int):Boolean={
    if(StringUtils.isEmpty(value)){
      true
    }else {
      var regxDecimal =
        """(-?(?:[1-9]\d{0,""" + len1 +"""}|0)"""
      if(len2>0) {
        regxDecimal +="""(?:\.\d{1,""" + len2 +"""})?"""
      }
      regxDecimal+=""")"""
      checkRegx(value, regxDecimal)
    }
  }

  /**
    *check the value if fix Decimal
    *
    * @param value
    * @param len1
    */
  protected def checkDecimal(value:String,len1:Int):Boolean={
    if(StringUtils.isEmpty(value)){
      true
    }else {
      val regxDecimal = """(-?(?:[1-9]\d{0,""" + len1 +"""}|0)(?:\.\d+)?)"""
      checkRegx(value, regxDecimal)
    }
  }

  /**
    * check the value if fix Decimal
    *
    * @param value
    */
  protected def checkDecimal(value:String):Boolean={
    if(StringUtils.isEmpty(value)){
      true
    }else {
      val regxDecimal = """(-?(?:[1-9]\d*|0)(?:\.\d+)?)"""
      checkRegx(value, regxDecimal)
    }
  }

  /**
    *
    */
  protected def checkRegx(value:String,regxString:String):Boolean={
    val regx = regxString.r
    value match{
      case regx(v)=>true
      case _ => false
    }
  }

}

object FieldValidator extends FieldsCheck {
  private def checkRequired(fieldType: IFFFieldType, fieldValue: String):Boolean={
    (!fieldType.required)||StringUtils.isNotEmpty(fieldValue)
  }

  private def checkMaxlength(fieldType: IFFFieldType, fieldValue: String):Boolean={
    fieldType match {
      case fieldType: IFFMaxlengthType => if(fieldType.maxlength>0){
        checkMaxlength(fieldValue,fieldType.maxlength,fieldType.chartSet)
      }else{
        true
      }
      case _ => true
    }
  }

  def validatBase(fieldType: IFFFieldType, fieldValue: String): Boolean ={
    checkRequired(fieldType,fieldValue)&& checkMaxlength(fieldType,fieldValue)
  }

  def validatCDecimal(fieldType: CDecimal,fieldValue: String):Boolean={
    validatBase(fieldType,fieldValue)&&
    checkDecimal(fieldValue,fieldType.precision-fieldType.scale,fieldType.scale)
  }

  def validatCInt(fieldType: CInteger,fieldValue: String):Boolean={
    validatBase(fieldType,fieldValue)&&checkInt(fieldValue)
  }

  def validatCDate(fieldType: CDate,fieldValue: String):Boolean={
    validatBase(fieldType,fieldValue)&&checkDate(fieldValue,fieldType.pattern)
  }

  def validatCTime(fieldType: CTime, fieldValue: String):Boolean={
    validatBase(fieldType,fieldValue)&&checkDate(fieldValue,fieldType.pattern)
  }

  def validatCTimestamp(fieldType: CTimestamp, fieldValue: String):Boolean={
    validatBase(fieldType,fieldValue)&&checkDate(fieldValue,fieldType.pattern)
  }

  def validatExpression(list: java.util.List[String],hashMap: java.util.HashMap[String,Any]):Boolean={
    if(list==null||list.size()==0) {
      true
    }else{
      import scala.collection.JavaConversions._
      for(exp<-list){
        val result=Ognl.getValue(exp.trim,hashMap)
        if(!result.asInstanceOf[Boolean]){
          return false
        }
      }
      true
    }

  }

  def main(args: Array[String]) {
    println(checkInt("954435"))
  }


}
