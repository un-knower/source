package com.datahandle.tran

import com.boc.iff.CommonFieldConvertor
import com.boc.iff.model.{IFFField, IFFFieldType}
import java.util.Date
import java.lang.{Double,Integer}

/**
  * Created by scutlxj on 2017/2/24.
  */
class FunctionExecutor extends Serializable{

  protected def execute[T <: AnyRef](fun:String,value: T, pattern:String*)
                               (implicit transformer: CommonFieldTransformer[T]): Any={
    fun match {
      case "to_char"=>transformer.to_char(value,pattern(0))
      case "to_date"=>transformer.to_date(value,pattern(0))
    }
  }

  protected def executeFunction(fun:String,value:Any, pattern:String*):Any={
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

  def to_char(value:Any,pattern:String):String={
    val newValue = executeFunction("to_char",value,pattern)
    if(newValue!=null){
      newValue.asInstanceOf[String]
    }else{
      null
    }
  }

  def to_char(value:Any):String={
    to_char(value,null)
  }

  def to_date(value:Any,pattern:String):Date={
    val newValue = executeFunction("to_date",value,pattern)
    if(newValue!=null){
      newValue.asInstanceOf[Date]
    }else{
      null
    }
  }


}
