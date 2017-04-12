package com.datahandle.tran

import java.sql.Date
import java.lang.{Double,Long}
import java.text.SimpleDateFormat

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
      case "to_number" => transformer.to_number(value)
      case "to_double" => transformer.to_double(value)
    }
  }

  protected def executeFunction(fun:String,value:Any, pattern:Any*):Any={
    if(value!=null) {
      value match {
        case value:String => execute(fun, value, pattern: _*)
        case value:Long => execute(fun, value, pattern: _*)
        case value:Double => execute(fun, value, pattern: _*)
        case value:Date => execute(fun, value, pattern: _*)
        case _ => execute(fun, value.asInstanceOf[String], pattern: _*)
      }
    }else{
      value
    }
  }

  def to_char(value:Any,pattern:String):String={
    val newValue = executeFunction("to_char",value,pattern)
    if(newValue!=null){
      newValue.asInstanceOf[String]
    }else{
      null
    }
  }

  def to_char(value:Any):String={
    to_char(value,"")
  }

  def to_date(value:Any,pattern:String):Date={
    val newValue = executeFunction("to_date",value,pattern)
    if(newValue!=null){
      new Date(newValue.asInstanceOf[java.util.Date].getTime)
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

  def to_number(value:Any):Long={
    executeFunction("to_number",value).asInstanceOf[Long]
  }

  def to_double(value:Any):Double={
    executeFunction("to_double",value).asInstanceOf[Double]
  }

  def trim(value:Any,pattern:String):String={
    val newValue = executeFunction("trim",value,pattern)
    if(newValue!=null){
      newValue.asInstanceOf[String]
    }else{
      ""
    }
  }

  def trim(value:Any):String={
    trim(value," ")
  }

  def ltrim(value:Any):String={
    ltrim(value," ")
  }

  def rtrim(value:Any):String={
    rtrim(value," ")
  }

  def ltrim(value:Any,pattern:String):String={
    val newValue = executeFunction("ltrim",value,pattern)
    if(newValue!=null){
      newValue.asInstanceOf[String]
    }else{
      ""
    }
  }

  def rtrim(value:Any,pattern:String):String={
    val newValue = executeFunction("rtrim",value,pattern)
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
    val format = new SimpleDateFormat("yyyy-MM-dd")
    Date.valueOf(format.format(new java.util.Date()))
  }

  def current_monthend(value:Any):Date={
    val newValue = executeFunction("current_monthend",value)
    if(newValue!=null){
      newValue.asInstanceOf[Date]
    }else{
      null
    }
  }

  def trunc(value:Any):Any={
    executeFunction("trunc",value,"")
  }

  def trunc(value:Any,pattern:String):Any={
    executeFunction("trunc",value,pattern)
  }

  def round(value:Any,pattern:String):Any={
    executeFunction("round",value,pattern)
  }

  def replace(value:Any,searchStr:String,replaceStr:String,pos:Int):String={
    val newValue = executeFunction("replace",value,searchStr,replaceStr,pos)
    if(newValue!=null){
      newValue.asInstanceOf[String]
    }else{
      ""
    }
  }







}
