package com.datahandle.tran

import java.text.{DecimalFormat, SimpleDateFormat}
import java.util.Date
import java.lang.Double
import scala.util.matching.Regex

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

  def sublenstring (fieldValue:T,startPos:Int,sublen:Int):String={
    throw new StageHandleException("round do not apply for type[%s]".format(fieldValue.getClass.getSimpleName))
  }


}

object CommonFieldTransformer {
  trait StringFieldTransformer extends CommonFieldTransformer[String]{
    override def to_char(fieldValue:String , pattern: String):String={
      if(fieldValue==null)
        throw new StageHandleException("to_char do not apply for null")
      if(StringUtils.isNotEmpty(pattern)){
        if(fieldValue.size>pattern.size)
          fieldValue
        else
          pattern.substring(0,pattern.size-fieldValue.size)+fieldValue
      }else
        fieldValue
    }

    override def to_date(fieldValue:String , pattern: String):Date={
      val retDate = pattern.toUpperCase match{
        case "YYYYMMDD" =>  new SimpleDateFormat("yyyyMMdd").parse(fieldValue)
        case "YYYY-MM-DD" => new SimpleDateFormat("yyyy-MM-dd").parse(fieldValue)
        case "YYYY/MM/DD" => new SimpleDateFormat("yyyy/MM/dd").parse(fieldValue)
        case "YYMMDD" => new SimpleDateFormat("yyMMdd").parse(fieldValue)
        case "YYYY/M/D" => new SimpleDateFormat("yyyy/M/d").parse(fieldValue)
        case _ => new SimpleDateFormat(pattern).parse(fieldValue)
      }
      retDate
    }

    override def substring(fieldValue:String, startPos:Int,endPos:Int):String={
      fieldValue.substring(startPos,endPos)
    }

    override def sublenstring(fieldValue:String, startPos:Int,subLength:Int):String={
      if(subLength>0)
        fieldValue.substring(startPos,startPos+subLength)
      else
        fieldValue.substring(startPos+subLength,startPos)
    }

    override def to_number(fieldValue:String):Int={
      fieldValue.toInt
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

    override def round(fieldValue: Double,pattern:String):Double = {
      var i=0
      var tens=1
      val p=pattern.toInt
      if(p>0){
         while(i < p){
           tens=tens*10
           i=i+1
         }
        println(tens)
        math.round(fieldValue*tens)*1.0/tens
      }else{
        while(i < (0-p)){
          tens=tens*10
          i=i+1
        }
        println(tens)
        math.round(fieldValue/tens)*tens
      }
    }

    override def to_char (fieldValue: Double,pattern:String):String={
      val ptn0 = new Regex("([+-])*([0,])*(.0[0,]*)")
      val ptn9 = new Regex("([+-])*([9,])*(.9[9,]*)")

      if(StringUtils.isEmpty((ptn0 findAllIn pattern).mkString(" "))
        &&StringUtils.isEmpty((ptn9 findAllIn pattern).mkString(" "))){
        throw new StageHandleException("pattern [%s] is invalid ".format(pattern))
      }

      val pattern1=pattern.replace('9','#').replace('+',' ').replace('-',' ').trim
      val pointAt = pattern1.indexOf(".")
      if(pattern1.substring(0,1)=="0" && pointAt>0 ){
        var l1=0
        var r1=0
        for(c<-pattern1.substring(0,pointAt).toArray){if(c==',')l1=l1+1}
        val len1 = pattern1.size
        for(c<-pattern1.substring(pointAt+1,len1).toArray){if(c==',')r1=r1+1}
        val len=len1-r1-l1
        val prec = len1 - pointAt -1-r1
        if(prec>0){
          var i=0
          var tens = 1
          while(i < prec){
            tens=tens*10
             i=i+1
          }
          val part1=math.floor(fieldValue)
          val part2=math.round((fieldValue-part1)*tens)
          val format1 = new DecimalFormat(pattern1.substring(0,pointAt))
          val format2 = new DecimalFormat(pattern1.substring(pointAt+1,len1))
          if(pattern.substring(0,1)=="+" || pattern.substring(0,1)=="-"){
            if(fieldValue>0.0)
              pattern.substring(0,1)+format1.format(part1)+"."+format2.format(part2)
            else
              format1.format(part1)+"."+format2.format(part2)
          }else{
            format1.format(part1)+"."+format2.format(part2)
          }
        }else{
          val format = new DecimalFormat(pattern1)
          if((pattern.substring(0,1)=="+" || pattern.substring(0,1)=="-")&&fieldValue>0.0){
            pattern.substring(0,1)+format.format(fieldValue)
          }else{
            format.format(fieldValue)
          }
       }
     }else{
        val format = new DecimalFormat(pattern1)
        if((pattern.substring(0,1)=="+" || pattern.substring(0,1)=="-")&&fieldValue>0.0){
          pattern.substring(0,1)+format.format(fieldValue)
        }else{
          format.format(fieldValue)
        }
      }
    }
  }
  implicit object DecimalTransformField extends DecimalFieldTransformer

  trait IntegerFieldTransformer extends CommonFieldTransformer[Integer] {
    override def to_char (fieldValue: Integer,pattern:String):String={
      if(StringUtils.isNotEmpty(pattern)) {
        val newPattern=pattern.replace('9','#').replace('+',' ').replace('-',' ').trim
        val format = new DecimalFormat(newPattern)
        if(pattern.substring(0,1)=="+" || pattern.substring(0,1)=="-"){
          if(fieldValue>0.0)
            "+"+format.format(fieldValue)
          else
            format.format(fieldValue)
        }else
          format.format(fieldValue)
      }else{
        fieldValue.toString
      }
    }
  }
  implicit object IntegerTransformField extends IntegerFieldTransformer

  trait DateFieldTransformer extends CommonFieldTransformer[Date] {
    override def to_char(fieldValue: Date,pattern:String) = {
      val charDay = pattern.toUpperCase match{
        case "YYYYMMDD" => new SimpleDateFormat("yyyyMMdd").format(fieldValue)
        case "YYYY-MM-DD" => new SimpleDateFormat("yyyy-MM-dd").format(fieldValue)
        case "YYYY/MM/DD" =>  new SimpleDateFormat("yyyy/MM/dd").format(fieldValue)
        case "YYMMDD" => new SimpleDateFormat("yy/MM/dd").format(fieldValue)
        case "YYYY/M/D" => new SimpleDateFormat("yyyy/M/d").format(fieldValue)
        case "YYYY" => new SimpleDateFormat("yyyy").format(fieldValue)
        case "MM" => new SimpleDateFormat("MM").format(fieldValue)
        case "DD" => new SimpleDateFormat("dd").format(fieldValue)
        case "M" => new SimpleDateFormat("M").format(fieldValue)
        case "D" => new SimpleDateFormat("d").format(fieldValue)
        case "WEEKDAY" => fieldValue.getDay.toString
        case "MONTHDAY" => new SimpleDateFormat("d").format(fieldValue)
        case "YEARDAY" => ((fieldValue.getTime-new SimpleDateFormat("yyyyMMdd").parse(new SimpleDateFormat("yyyy").format(fieldValue)+"01"+"01").getTime)/(1000*3600*24)+1).toString
        case _ => fieldValue.toString
      }
      charDay
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
