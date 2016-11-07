package com.boc.iff

import java.io.{FileInputStream, InputStream}
import java.util.Properties

import org.apache.commons.lang.StringUtils

import scala.collection.mutable.ArrayBuffer



/**
  *
  */
class SpecialCharConvertor extends Serializable{
  var defaultTargetChar:Char = 0x00
  var sourceChars:Array[Char] = null
  var targetChars:Array[Char] = null


  def this(specialCharFile:String){
    this()
    val pro =  new Properties();
    var is:InputStream = null;
    try{
      is =  new FileInputStream(specialCharFile)
      pro.load(is)
      is.close()
    }catch {
      case e:Exception=>
        if(is!=null)is.close()
        throw e
    }
    sourceChars = new Array[Char](pro.keySet().size())
    targetChars = new Array[Char](pro.keySet().size())
    var key:String = null
    var target:String = null
    val keys = pro.keySet().iterator()
    var index:Int = 0
    while(keys.hasNext){
      key = keys.next().toString
      target = pro.getProperty(key)
      if(key.length()==1){
        sourceChars(index)=key.charAt(0);
      }else{
        if(key.startsWith("0x")){
          key = key.substring(2);
        }
        sourceChars(index)=hexStringToChar(key);
      }
      if(StringUtils.isNotEmpty(target)&&target.length()==1){
        targetChars(index) = target.charAt(0);
      }else{
        if(StringUtils.isNotEmpty(target)&&target.startsWith("0x")){
          target = target.substring(2);
        }
        targetChars(index)=hexStringToChar(target);
      }
      index += 1
    }
  }

  def this(specialCharFile:String,defaultTargetChar:Char){
    this(specialCharFile)
    this.defaultTargetChar = defaultTargetChar
  }

  /**
    * 转换文字
    * @param str
    * @return
    */
  def convert(str:String):String={
    val srcChar = str.toCharArray
    val tarChar = new ArrayBuffer[Char]()
    var targetChar:Char = defaultTargetChar;
    var isSpecialChar:Boolean = false;
    for(i<-0 until srcChar.length){
      isSpecialChar = false;
      for(j<-0 until sourceChars.length){
        if(srcChar(i)==sourceChars(j)){
          targetChar=targetChars(j)
          isSpecialChar = true
        }
      }
      if(isSpecialChar){
        if(0x00!=targetChar){
          tarChar+=targetChar
        }
      }else{
        tarChar+=srcChar(i)
      }
    }
    return new String(tarChar.toArray)
  }

  private def hexStringToChar(str:String):Char={
    if (str == null || str.equals("")) {
      return this.defaultTargetChar;
    }
    var s = str.replace(" ", "");
    val baKeyword = new Array[Byte](s.length/2)
    for (i<-0 until  baKeyword.length) {
      try {
        baKeyword(i) = (0xff & Integer.parseInt(
          s.substring(i * 2, i * 2 + 2), 16)).toByte;
      } catch{
        case e:Exception=>e.printStackTrace();
      }
    }
    try {
      s = new String(baKeyword, "gbk");
      new String();
    } catch{
      case e:Exception=>e.printStackTrace();
    }
    if(s!=null&&s.length()>0){
      return s.charAt(0);
    }else{
      return this.defaultTargetChar;
    }
  }


}
