package com.boc.iff.load

import java.io.{BufferedReader, InputStreamReader}
import java.nio.charset.Charset

import com.boc.iff.SpecialCharConvertor
import com.boc.iff.model.{IFFFileInfo, IFFMetadata}
import org.apache.commons.lang3.StringUtils

import scala.collection.mutable.ArrayBuffer


/**
  * Created by scutlxj on 2016/11/16.
  */
abstract class DataFileProcessor extends Serializable{
  var specialCharConvertor:SpecialCharConvertor = null
  var iffMetadata:IFFMetadata = null
  var iffFileInfo:IFFFileInfo = null
  var lengthOfLineEnd:Int = 1
  var charset:Charset = null
  var needConvertSpecialChar:Boolean = false
  protected var blockSize:Long = 0
  protected var inputStream:java.io.InputStream = null
  protected var currentBlockReadedBytesCount:Long = 0

  def open(inputStream:java.io.InputStream,blockSize:Long):Unit={
    this.blockSize = blockSize
    this.inputStream = inputStream
    this.currentBlockReadedBytesCount = 0
  }

  def close(): Unit ={
    try{
      if(inputStream!=null){
        inputStream.close()
      }
    }catch {
      case e:Exception => e.printStackTrace()
    }
  }

  def getLineFields():Array[String]

  def haveMoreLine():Boolean={
    currentBlockReadedBytesCount<blockSize
  }

}


class UnfixDataFileProcessor extends DataFileProcessor{

  protected var br:java.io.BufferedReader = null

  override def open(inputStream:java.io.InputStream,blockSize:Long):Unit={
    super.open(inputStream,blockSize)
    br = new BufferedReader(new InputStreamReader(inputStream,charset))
  }

  override def close(): Unit ={
    try{
      if(br!=null){
        br.close()
      }
    }catch {
      case e:Exception => e.printStackTrace()
    }
    super.close()
  }

  def getLineFields():Array[String]={
    if(this.currentBlockReadedBytesCount<blockSize){
      val fields = new ArrayBuffer[String]
      var currentLine = br.readLine()
      currentBlockReadedBytesCount += currentLine.getBytes(charset).length + lengthOfLineEnd
      if (StringUtils.isNotEmpty(currentLine.trim)) {
        if(needConvertSpecialChar){
          currentLine = specialCharConvertor.convert(currentLine)
        }
        val lineSeq = StringUtils.splitByWholeSeparatorPreserveAllTokens(currentLine, iffMetadata.srcSeparator)
        for(s<-lineSeq){
          fields += s
        }
      }
      fields.toArray
    }else{
      null
    }
  }

}

class FixDataFileProcessor extends DataFileProcessor{
  override def getLineFields(): Array[String] = {
    if(this.currentBlockReadedBytesCount<blockSize){
      val fields = new ArrayBuffer[String]
      val recordLen = iffFileInfo.recordLength+lengthOfLineEnd
      val recordBytes = new Array[Byte](recordLen)
      var recordLength: Int = 0
      while (recordLength < recordLen) {
        val readLength = inputStream.read(
          recordBytes, recordLength, recordLen - recordLength)
        if (readLength == -1) {
          recordLength = recordLen
        } else {
          recordLength += readLength
        }
      }
      currentBlockReadedBytesCount+=recordLength
      for (iffField <- iffMetadata.body.fields if (!iffField.virtual)) {
        var fieldVal = new String(java.util.Arrays.copyOfRange(recordBytes, iffField.startPos, iffField.endPos + 1), iffMetadata.sourceCharset)
        if (StringUtils.isNotBlank(fieldVal)&&needConvertSpecialChar) {
          fieldVal = specialCharConvertor.convert(fieldVal)
        }
        fields += fieldVal
      }
      fields.toArray
    }else{
      null
    }
  }
}