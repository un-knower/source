package com.datahandle.tran

import java.text.{DecimalFormat, SimpleDateFormat}
import java.text.ParseException
import java.util.{Date,Calendar}
import java.lang.Double
import scala.util.matching.Regex

import com.boc.iff.exception.{StageHandleException, StageInfoErrorException}
import org.apache.commons.lang3.StringUtils

/**
  * Created by scutlxj on 2017/2/22.
  * Modified by xp on 2017/03/23
  */
@annotation.implicitNotFound(msg = "No implicit CommonFieldTransformer defined for ${T}.")
sealed trait CommonFieldTransformer[T<:Any]  {

  val valueFormat = "####################.##########"

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

  def to_number(fieldValue:T):Int={
    throw new StageHandleException("to_number do not apply for type[%s]".format(fieldValue.getClass.getSimpleName))
  }

  def to_double(fieldValue:T):Double={
    throw new StageHandleException("to_double do not apply for type[%s]".format(fieldValue.getClass.getSimpleName))
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
      val df = pattern.toUpperCase match{
        case "YYYYMMDD" =>  new SimpleDateFormat("yyyyMMdd")
        case "YYYY-MM-DD" => new SimpleDateFormat("yyyy-MM-dd")
        case "YYYY/MM/DD" => new SimpleDateFormat("yyyy/MM/dd")
        case "YYMMDD" => new SimpleDateFormat("yyMMdd")
        case "YYYY/M/D" => new SimpleDateFormat("yyyy/M/d")
        case _ => new SimpleDateFormat(pattern)
      }
      df.setLenient(false)
      df.parse(fieldValue)
    }

    override def substring(fieldValue:String, startPos:Int,subLength:Int):String={
      if(subLength>0)
        fieldValue.substring(startPos,startPos+subLength)
      else
        fieldValue.substring(startPos+subLength+1,startPos+1)
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

    override def to_double(fieldValue:String):Double={
      fieldValue.toDouble
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
        for(i<-0 until fieldValue.length if result==null) if(pc!=fieldValue.charAt(i)) result = fieldValue.substring(i)
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
        val len = fieldValue.length
        for(i<- 0 until len if result==null) {
          if(pc!=fieldValue.charAt(len-i-1)) {
            result = fieldValue.substring(0,len-i)
          }
        }
        result
      }else{
        ""
      }
    }

    override def replace(fieldValue:String,searchStr:String,replaceStr:String,pos:Int):String={
      if(pos==0){
        fieldValue.replace(searchStr,replaceStr)
      }else {
        var endFlag=""
        if(searchStr.substring(searchStr.size-1,searchStr.size)!="1")
          endFlag="1"
        else
          endFlag="2"
        if(pos>0){
          if(searchStr!=replaceStr){
            val strArray = (fieldValue+endFlag).split(searchStr)
            if( strArray.size <= pos )
              fieldValue
            else{
              var retValue=""
              for( c<-0 to strArray.size-1 ){
                if(c==pos-1)
                  retValue=retValue+strArray(c)+replaceStr
                else if(c!=strArray.size-1)
                  retValue=retValue+strArray(c)+searchStr
                else if( strArray(c).size >=2 )
                  retValue=retValue+strArray(c).substring(0,strArray(c).size-1)
              }
              retValue
            }
          }else{
            fieldValue
          }
        }else{
          replace(fieldValue.reverse,searchStr,replaceStr,0-pos).reverse
        }
      }
    }
  }
  implicit object StringTransformField extends StringFieldTransformer


  trait DecimalFieldTransformer extends CommonFieldTransformer[Double] {

    override def round(fieldValue: Double,pattern:String):Double = {
      val patt = pattern.trim
      var decLen = 0
      if(patt.indexOf(".")>0 && (patt.size>patt.indexOf(".")+1)){
        decLen = patt.substring(patt.indexOf(".")+1,patt.size).toArray.filter(_!=',' ).size
      }else{
        decLen = patt.toInt
      }
      BigDecimal(fieldValue).setScale(decLen,BigDecimal.RoundingMode.HALF_UP).toDouble
    }

    override def trunc(fieldValue:Double,pattern:String):Long={
      math.floor(fieldValue).toLong
    }

    def pointLeft_to_char(fieldValue:Double,intPattern:String):String={
      val valueStr = new DecimalFormat(valueFormat).format(fieldValue)
      val intLen = if (valueStr.indexOf(".")>=0) valueStr.indexOf(".") else valueStr.size
      val intStr = valueStr.substring(0,intLen)
      val pointLoc = intPattern.size

      var subSize:Int = 0
      var flag = true
      for( k <-  1 to pointLoc if flag ){
        if(intPattern.substring(pointLoc-k,pointLoc-k+1)==",")
          flag = false
        else
          subSize = subSize+1
      }
      flag = true
      var vLen =intLen
      var intPart = ""
      while(flag){
        for( k <- 1 to subSize if flag ){
          if( k<=vLen ){
            intPart  = intStr.substring(vLen-k,vLen-k+1) + intPart
          }else{
            flag = false
          }
        }
        if( flag && vLen>=subSize){
          vLen = vLen-subSize
          if(vLen>0)
            intPart = "," + intPart
        }
      }
      intPart
    }

    def pointRight_to_char(fieldValue:Double,decPattern:String):String={
      val valueStr = new DecimalFormat(valueFormat).format(fieldValue)
      val intLen = if(valueStr.indexOf(".") >=0) valueStr.indexOf(".")  else valueStr.size
      val decLen = if(valueStr.size-intLen -1 >=0) valueStr.size-intLen -1 else decPattern.toArray.filter(_!=',' ).size
      val decStr = if(valueStr.size-intLen -1 >=0) valueStr.substring(intLen+1,valueStr.size) else "0000000000"
      val decPatternLen = decPattern.size

      var subSize:Int = 0
      var flag = true
      for( k <-  1 to decPatternLen if flag ){
        if(decPattern.substring(k-1,k)==",")
          flag = false
        else
          subSize = subSize+1
      }
      flag = true
      var vLen =decLen
      var decPart = ""
      var i:Int = 0
      while(flag){
        for( k <- 1 to subSize if flag ){
          if( k<=vLen ){
            decPart  = decPart + decStr.substring(i,i+1)
            i=i+1
          }else{
            flag = false
          }
        }
        if( flag && vLen>=subSize){
          vLen = vLen-subSize
          if(vLen>0)
            decPart  = decPart + ","
        }
      }
      decPart
    }

    override def to_char (fieldValue: Double,pattern:String):String={
      val ptn0 = new Regex("([+-])*([0,])*(.0[0,]*)")
      val ptn9 = new Regex("([+-])*([9,])*(.9[9,]*)")
      if((StringUtils.isEmpty((ptn0 findAllIn pattern).mkString(" ")) &&StringUtils.isEmpty((ptn9 findAllIn pattern).mkString(" ")))
      ||( pattern.indexOf("0")>0 && pattern.indexOf("9")>0 ) ) {
        throw new StageHandleException("pattern [%s] is invalid ".format(pattern))
      }

      var signSymbol = ""
      var patt = pattern.trim
      if(patt.substring(0,1)=="+"|| patt.substring(0,1)=="-"){
        signSymbol=patt.substring(0,1)
        if(fieldValue > 0.0)
          signSymbol = "+"
        else
          if(fieldValue < 0.0) signSymbol = ""       
        patt=patt.substring(1,patt.size)
      }
      if(patt.indexOf(".")>0){
        val intPattLen = patt.indexOf(".")
        val intPatt = patt.substring(0,intPattLen)
        var intPart = ""
        if(intPatt.indexOf(",")>0){
          intPart = pointLeft_to_char(fieldValue,intPatt)
        }else{
          intPart = new DecimalFormat(intPatt.replace('9','#')).format(math.floor(fieldValue))
        }

        val decPatt = patt.substring(intPattLen+1,patt.size)
        var decPart = ""
        if(decPatt.indexOf(",")>0){
          decPart = "." + pointRight_to_char(fieldValue,decPatt)
        }else{
          val valueStr = new DecimalFormat(valueFormat).format(fieldValue)
          val intLen = valueStr.indexOf(".")
          var decStr = ""
          if(intLen>=0)
            decStr = "0."+valueStr.substring(intLen+1,valueStr.size)
          else
             decStr = "0."+"0000000000"
          decPart = new DecimalFormat("."+decPatt.replace('9','#')).format(round(decStr.toDouble,pattern))
        }
        signSymbol+intPart+decPart
      }else{
        val ret = new DecimalFormat(patt.replace('9','#')).format(fieldValue)
        signSymbol+ret
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
        new DecimalFormat(valueFormat).format(fieldValue)
      }
    }
  }
  implicit object IntegerTransformField extends IntegerFieldTransformer

  trait LongFieldTransformer extends CommonFieldTransformer[Long] {
    override def to_char (fieldValue: Long,pattern:String):String={
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
        new DecimalFormat(valueFormat).format(fieldValue)
      }
    }
  }
  implicit object LongTransformField extends LongFieldTransformer

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

    override def current_monthend(fieldValue:Date):Date={
      val dateFormat = new SimpleDateFormat("yyyyMMdd")
      val ca = Calendar.getInstance()
      ca.setTime(dateFormat.parse(to_char(fieldValue,"YYYY")+to_char(fieldValue,"MM")+"01"))
      ca.add(Calendar.MONTH,1)
      ca.add(Calendar.DAY_OF_MONTH,-1)
      ca.getTime
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
