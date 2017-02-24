package com.datahandle.tran

import java.text.{DecimalFormat, SimpleDateFormat}
import java.util.Date
import java.lang.Double

import com.boc.iff.exception.StageInfoErrorException
import org.apache.commons.lang3.StringUtils

/**
  * Created by scutlxj on 2017/2/22.
  */
@annotation.implicitNotFound(msg = "No implicit CommonFieldTransformer defined for ${T}.")
sealed trait CommonFieldTransformer[T<:Any]  {

  def to_char (fieldValue: T,pattern:String):String={fieldValue.toString}
  def to_date(fieldValue:T , pattern: String):Date={
    throw new StageInfoErrorException("to_date function pattern type not right")
  }

}

object CommonFieldTransformer {
  trait StringFieldTransformer extends CommonFieldTransformer[String]{
    override def to_date(fieldValue:String , pattern: String):Date={
      val format = new SimpleDateFormat(pattern)
      format.parse(fieldValue)
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
        val format = new SimpleDateFormat(pattern)
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
  }
  implicit object DateTransformField extends DateFieldTransformer

  def apply[T<:Any](implicit transformer: CommonFieldTransformer[T]) = {
    transformer
  }

  /**
    * @author www.birdiexx.com
    */
}
