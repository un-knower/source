package com.boc.iff

import java.nio.charset.{Charset, CharsetDecoder, CodingErrorAction, UnsupportedCharsetException}
import java.nio.{ByteBuffer, CharBuffer}
import java.text.SimpleDateFormat
import java.util.GregorianCalendar

import com.ibm.icu.charset.CharsetProviderICU

/**
  * Created by cvinc on 2016/6/8.
  */
object IFFUtils {

  object FileMode extends Enumeration{
    type ValueType = Value
    val local = Value(0, "local")
    val dfs = Value(1, "dfs")
  }


  private val HEXES: String = "0123456789ABCDEF"
  private val SIZE_REGEX = """^([\d]+)([TtGgMmKk]?)$""".r

  def getHex(raw: Array[Byte]): String = {
    if (raw == null) {
      return null
    }
    val hex = new StringBuilder(2 * raw.length)
    for (b <- raw) {
      hex += HEXES.charAt((b & 0xF0) >> 4)
      hex += HEXES.charAt(b & 0x0F)
    }
    hex.toString
  }

  def byteDecode(decoder: CharsetDecoder, decodeByte: Array[Byte], lineEsc: Boolean): String = {
    val byteBuffer = ByteBuffer.wrap(decodeByte)
    val charBuffer = CharBuffer.allocate(3 * decodeByte.length)
    charBuffer.clear
    decoder.reset
    decoder.onUnmappableCharacter(CodingErrorAction.REPLACE)
    decoder.onMalformedInput(CodingErrorAction.REPLACE)
    decoder.decode(byteBuffer, charBuffer, true)
    charBuffer.flip
    if (lineEsc) {
      def putSpace(index: Int) = { charBuffer.put(index, ' ') }
      for(i<-0 until charBuffer.length()){
        charBuffer.charAt(i) match {
          case '\t' => putSpace(i)
          case '\r' => putSpace(i)
          case '\n' => putSpace(i)
          case _ =>
        }
      }
    }
    charBuffer.toString
  }

  def getCharset(charsetName: String): Charset = {
    val logger = new ECCLogger()
    try {
      Charset.forName(charsetName)
    } catch {
      case e: UnsupportedCharsetException =>
        logger.debug("-CNV1001", "Java unsupported charset detected, switch to ICU CharsetProviderICU :" + charsetName)
        val charsetProviderICU = new CharsetProviderICU()
        val charsetFileUrl = this.getClass.getClassLoader.getResource("conversion_table" + "/" + charsetName + ".cnv")
        logger.info("-CNV1001", "Charset File: " + charsetFileUrl)
        val charset = charsetProviderICU.charsetForName(charsetName, "conversion_table", this.getClass.getClassLoader)
        if (charset == null) {
          val desc = "CharsetProviderICU not found :" + charsetName
          logger.error("-CNV1001", desc)
          throw new Exception(desc)
        }
        charset
    }
  }

  def getSize(sizeText: String): Int = {
    println("sizeTextsizeTextsizeText"+sizeText)
    val SIZE_REGEX(valueText, unit) = sizeText
    val value = valueText.toInt
    val unitPow = unit.toLowerCase match {
      case "t" => 4
      case "g" => 3
      case "m" => 2
      case _ => 1
    }
    println("value:"+value+" unitPow:"+unitPow)
    value * math.pow(1024, unitPow).toInt
  }

  def addDays(date:String,num:Int):String={
    val d1:String = date.substring(0, 4)
    val d2:String = date.substring(4, 6)
    val d3:String = date.substring(6, 8)
    val mortgage = new GregorianCalendar(Integer.parseInt(d1), Integer.parseInt(d2) - 1, Integer.parseInt(d3))
    mortgage.add(5, num)
    val format:SimpleDateFormat = new SimpleDateFormat(IFFConversionConfig.ACCOUNT_DATE_PATTERN)
    return format.format(mortgage.getTime())
  }

  def dateToString(date:java.util.Date):String={
    val format:SimpleDateFormat = new SimpleDateFormat(IFFConversionConfig.ACCOUNT_DATE_PATTERN)
    return format.format(date)
  }

}
