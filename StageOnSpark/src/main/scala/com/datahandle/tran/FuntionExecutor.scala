package com.datahandle.tran

import com.boc.iff.CommonFieldConvertor
import com.boc.iff.model.{IFFField, IFFFieldType}
import java.util.Date
import java.lang.{Double, Integer}

import com.boc.iff.exception.StageHandleException
import org.apache.commons.lang3.StringUtils

import scala.collection.mutable
import java.lang.reflect.Method

/**
  * Created by scutlxj on 2017/2/24.
  */
class FunctionExecutor extends Serializable{
  //val methodMap:mutable.HashMap[CommonFieldTransformer,mutable.HashMap[String,Method]] = new mutable.HashMap[CommonFieldTransformer,mutable.HashMap[String,Method]]()

  protected def execute[T <: AnyRef](fun:String,value: T, pattern:Any*)
                               (implicit transformer: CommonFieldTransformer[T]): Any={
    fun match {
      case "to_char"=>transformer.to_char(value,pattern(0).asInstanceOf[String])
      case "to_date"=>transformer.to_date(value,pattern(0).asInstanceOf[String])
      case "substring"=>transformer.substring(value,pattern(0).asInstanceOf[Int],pattern(1).asInstanceOf[Int])
      case "to_uppercase"=>transformer.to_uppercase(value)
      case "to_lowercase"=>transformer.to_lowercase(value)
      case "length"=>transformer.length(value)
      case "trim" => transformer.trim(value,pattern(0).asInstanceOf[String])
      case "ltrim" => transformer.ltrim(value,pattern(0).asInstanceOf[String])
      case "rtrim" => transformer.rtrim(value,pattern(0).asInstanceOf[String])
      case "current_monthend" => transformer.current_monthend(value)
      case "trunc" => transformer.trunc(value,pattern(0).asInstanceOf[String])
      case "round" => transformer.round(value,pattern(0).asInstanceOf[String])
      case "replace" => transformer.round(value,pattern(0).asInstanceOf[String])
    }
  }

  protected def executeFunction(fun:String,value:Any, pattern:Any*):Any={
    if(value!=null) {
      value match {
        case value:String => execute(fun, value, pattern: _*)
        case value:Integer => execute(fun, value, pattern: _*)
        case value:Double => execute(fun, value, pattern: _*)
        case value:Date => execute(fun, value, pattern: _*)
        case _ => execute(fun, value.asInstanceOf[String], pattern: _*)
      }
    }else{
      value
    }
  }

  def to_char(value:Any,pattern:String*):String={
    val ptn = if(pattern==null||pattern.length==0)"" else pattern(0)
    val newValue = executeFunction("to_char",value,ptn)
    if(newValue!=null){
      newValue.asInstanceOf[String]
    }else{
      null
    }
  }

  def to_date(value:Any,pattern:String):Date={
    val newValue = executeFunction("to_date",value,pattern)
    if(newValue!=null){
      newValue.asInstanceOf[Date]
    }else{
      null
    }
  }

  def to_uppercase(value:Any):String={
    val newValue = executeFunction("to_uppercase",value)
    if(newValue!=null){
      newValue.asInstanceOf[String]
    }else{
      null
    }
  }

  def to_lowercase(value:Any):String={
    val newValue = executeFunction("to_lowercase",value)
    if(newValue!=null){
      newValue.asInstanceOf[String]
    }else{
      null
    }
  }

  def substring(value:Any,startPos:Int,length:Int):String={
    val newValue = executeFunction("substring",value,startPos,length)
    if(newValue!=null){
      newValue.asInstanceOf[String]
    }else{
      null
    }
  }

  def length(value:Any):Int={
    val newValue = executeFunction("length",value)
    if(newValue!=null){
      newValue.asInstanceOf[Int]
    }else{
      0
    }
  }

  def to_number(value:Any):Any={
    executeFunction("to_number",value)
  }

  def trim(value:Any,pattern:String*):String={
    val ptn = if(pattern==null||pattern.length==0) " " else pattern(0)
    val newValue = executeFunction("trim",value,ptn)
    if(newValue!=null){
      newValue.asInstanceOf[String]
    }else{
      ""
    }
  }

  def ltrim(value:Any,pattern:String*):String={
    val ptn = if(pattern==null||pattern.length==0) " " else pattern(0)
    val newValue = executeFunction("ltrim",value,ptn)
    if(newValue!=null){
      newValue.asInstanceOf[String]
    }else{
      ""
    }
  }

  def rtrim(value:String,pattern:String*):String={
    val ptn = if(pattern==null||pattern.length==0) " " else pattern(0)
    val newValue = executeFunction("rtrim",value,ptn)
    if(newValue!=null){
      newValue.asInstanceOf[String]
    }else{
      ""
    }

  }


  /**
    * 函数: 返回当天日期
    * @return
    */
  def current_date():Date={
    null
  }

  def current_monthend(value:Any):Date={
    val newValue = executeFunction("current_monthend",value)
    if(newValue!=null){
      newValue.asInstanceOf[Date]
    }else{
      null
    }
  }

  def trunc(value:Any,pattern:String*):Any={
    val pt = if(pattern==null||pattern.length==0) "" else pattern(0)
    executeFunction("trunc",value,pt)
  }

  def round(value:Any,pattern:String):Any={
    executeFunction("round",value,pattern)
  }

  def replace(value:Any,searchStr:String,replaceStr:String,pos:Int*):String={
    val ps = if(pos==null||pos.length==0) 0 else pos(0)
    val newValue = executeFunction("replace",value,searchStr,replaceStr,ps)
    if(newValue!=null){
      newValue.asInstanceOf[String]
    }else{
      ""
    }
  }







}
