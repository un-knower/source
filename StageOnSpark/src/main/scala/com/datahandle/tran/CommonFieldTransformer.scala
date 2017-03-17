package com.datahandle.tran

import java.text.{DecimalFormat, SimpleDateFormat}
import java.util.Date
import java.lang.Double

import com.boc.iff.exception.{StageHandleException, StageInfoErrorException}
import org.apache.commons.lang3.StringUtils

/**
  * Created by scutlxj on 2017/2/22.
  */
@annotation.implicitNotFound(msg = "No implicit CommonFieldTransformer defined for ${T}.")
sealed trait CommonFieldTransformer[T<:Any]  {

  def to_char (fieldValue: T,pattern:String):String={fieldValue.toString}

  def to_date(fieldValue:T , pattern: String):Date={
    throw new StageHandleException("to_date do not apply for type[%s]".format(fieldValue.getClass.getSimpleName))
  }

  def to_uppercase(fieldValue:T):String={
    throw new StageHandleException("to_uppercase do not apply for type[%s]".format(fieldValue.getClass.getSimpleName))
  }

  def to_lowercase(fieldValue:T):String={
    throw new StageHandleException("to_lowercase do not apply for type[%s]".format(fieldValue.getClass.getSimpleName))
  }

  def substring(fieldValue:T , startPos:Int,length:Int):String={
    throw new StageHandleException("substring do not apply for type[%s]".format(fieldValue.getClass.getSimpleName))
  }

  def length(fieldValue:T):Int={
    fieldValue.toString.length
  }

  def to_number(fieldValue:T):Any={
    throw new StageHandleException("to_number do not apply for type[%s]".format(fieldValue.getClass.getSimpleName))
  }

  def trim(fieldValue:T,pattern:String):String={
    throw new StageHandleException("trim do not apply for type[%s]".format(fieldValue.getClass.getSimpleName))
  }

  def ltrim(fieldValue:T,pattern:String):String={
    throw new StageHandleException("ltrim do not apply for type[%s]".format(fieldValue.getClass.getSimpleName))
  }

  def rtrim(fieldValue:T,pattern:String):String={
    throw new StageHandleException("rtrim do not apply for type[%s]".format(fieldValue.getClass.getSimpleName))
  }

  def current_monthend(fieldValue:T):Date={
    throw new StageHandleException("current_monthend do not apply for type[%s]".format(fieldValue.getClass.getSimpleName))
  }

  def trunc(fieldValue:T,pattern:String):Any={
    throw new StageHandleException("trunc do not apply for type[%s]".format(fieldValue.getClass.getSimpleName))
  }

  def round(fieldValue:T,pattern:String):Any={
    throw new StageHandleException("round do not apply for type[%s]".format(fieldValue.getClass.getSimpleName))
  }

  def replace(fieldValue:T,searchStr:String,replaceStr:String,pos:Int):String={
    throw new StageHandleException("replace do not apply for type[%s]".format(fieldValue.getClass.getSimpleName))
  }


}

object CommonFieldTransformer {
  trait StringFieldTransformer extends CommonFieldTransformer[String]{
    override def to_date(fieldValue:String , pattern: String):Date={
      val format = new SimpleDateFormat(pattern)
      format.parse(fieldValue)
    }

    override def substring(fieldValue:String, startPos:Int,endPos:Int):String={
      fieldValue.substring(startPos,endPos)
    }

    override def to_uppercase(fieldValue:String):String={
      fieldValue.toUpperCase
    }

    override def to_lowercase(fieldValue:String):String={
      fieldValue.toLowerCase
    }

    override def trim(fieldValue:String , pattern: String):String={
      ltrim(rtrim(fieldValue,pattern),pattern)
    }

    override def ltrim(fieldValue:String , pattern: String):String={
      if(StringUtils.isNotBlank(fieldValue)){
        if(pattern.length>1){
          throw new StageHandleException("Trim pattern should be one char")
        }
        val pc = pattern.charAt(0)
        var result:String = null
        for(i<-0 until fieldValue.length if result!=null) if(pc!=fieldValue.charAt(i)) result = fieldValue.substring(i)
        result
      }else{
        ""
      }
    }

    override def rtrim(fieldValue:String , pattern: String):String={
      if(StringUtils.isNotBlank(fieldValue)){
        if(pattern.length>1){
          throw new StageHandleException("Trim pattern should be one char")
        }
        val pc = pattern.charAt(0)
        var result:String = null
        for(i<- fieldValue.length until 0 if result!=null) if(pc!=fieldValue.charAt(i)) result = fieldValue.substring(0,i)
        result
      }else{
        ""
      }
    }

  }
  implicit object StringTransformField extends StringFieldTransformer



  trait DecimalFieldTransformer extends CommonFieldTransformer[Double] {
    override def to_char (fieldValue: Double,pattern:String):String={
      if(StringUtils.isNotEmpty(pattern)) {
        val format = new DecimalFormat(pattern)
        format.format(fieldValue)
      }else{
        fieldValue.toString
      }
    }

  }
  implicit object DecimalTransformField extends DecimalFieldTransformer

  trait IntegerFieldTransformer extends CommonFieldTransformer[Integer] {
    override def to_char (fieldValue: Integer,pattern:String):String={
      if(StringUtils.isNotEmpty(pattern)) {
        val format = new DecimalFormat(pattern)
        format.format(fieldValue)
      }else{
        fieldValue.toString
      }
    }
  }
  implicit object IntegerTransformField extends IntegerFieldTransformer

  trait DateFieldTransformer extends CommonFieldTransformer[Date] {
    override def to_char(fieldValue: Date,pattern:String) = {
      if(StringUtils.isNotEmpty(pattern)) {
        val format = new SimpleDateFormat(pattern)
        format.format(fieldValue)
      }else{
        fieldValue.toString
      }
    }

    override def length(fieldValue:Date):Int={
      throw new StageInfoErrorException("length function type not right")
    }
  }
  implicit object DateTransformField extends DateFieldTransformer

  def apply[T<:Any](implicit transformer: CommonFieldTransformer[T]) = {
    transformer
  }

  /**
    * @author www.birdiexx.com
    */
}
