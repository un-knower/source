package com.boc.iff.load

import java.io.{BufferedReader, FileInputStream, InputStreamReader}
import java.util.Properties
import java.util.concurrent.LinkedBlockingQueue
import java.util.zip.GZIPInputStream

import com.boc.iff.IFFConversion._
import com.boc.iff.exception._
import com.boc.iff.model._
import com.boc.iff.{CommonFieldConvertorContext, CommonFieldValidatorContext, ECCLogger, IFFUtils}
import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.yarn.conf.YarnConfiguration

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
  * @author www.birdiexx.com
  */
class MutilUnfixedConversionOnSparkJob
  extends MutilConversionOnSparkJob[MutilConversionOnSparkConfig] {

  protected def createBlockPositionQueue(filePath:String): java.util.concurrent.LinkedBlockingQueue[(Int, Long, Int)] = {
    //块大小至少要等于数据行大小
    val blockSize = math.max(iffConversionConfig.blockSize, 0)
    logger.info("blockSize","blockSize:"+blockSize)
    val blockPositionQueue = new LinkedBlockingQueue[(Int, Long, Int)]()
    var totalBlockReadBytesCount: Long = 0
    val iffFileInputStream = openIFFFileBufferedInputStream(filePath, fileGzipFlgMap(filePath), iffConversionConfig.readBufferSize)
    var blockIndex: Int = 0
    var endOfFile = false
    val br = new BufferedReader(new InputStreamReader(iffFileInputStream.asInstanceOf[java.io.InputStream],IFFUtils.getCharset(iffMetadata.sourceCharset)))
    var currentLineLength: Int = 0
    var countLineNumber: Int = 0
    var blankLineNumber: Int = 0
    val needCheckBlank: Boolean = if(iffConversionConfig.fileMaxBlank==0) false else true

    logger.info("chartSet","chartSet"+iffMetadata.sourceCharset)
    while (!endOfFile) {
      var currentBlockReadBytesCount: Int = 0
      var canRead = true
      currentBlockReadBytesCount += currentLineLength
      while (canRead) {
        val lineStr = br.readLine()
        if(lineStr!=null){
          if(lineStr.startsWith(iffConversionConfig.fileEOFPrefix)){
            if(lineStr.startsWith(iffConversionConfig.fileEOFPrefix+"RecNum")){
              val recNum = lineStr.substring((iffConversionConfig.fileEOFPrefix+"RecNum=").length)
              if(recNum.toInt!=countLineNumber){
                logger.error("file "+iffConversionConfig.filename+ " number is not right "+recNum.toInt+countLineNumber,"file number is not right")
                throw RecordNumberErrorException("file " + iffConversionConfig.filename + " record number is not right,Expect record number:"+ countLineNumber+" Actually record number:" + recNum.toInt )
              }
            }
          }else {//. 检查记录是否空行
            if (StringUtils.isEmpty(lineStr.trim)) {
              blankLineNumber += 1
              if (needCheckBlank && blankLineNumber > iffConversionConfig.fileMaxBlank) {
                logger.error("file " + iffConversionConfig.filename + " blank number error :" + blankLineNumber + " iffMetadata.body" + iffMetadata.body.getSourceLength, "blank number error")
                throw MaxBlankNumberException("File blank number is bigger than limited, File blank number:" + blankLineNumber + ", file blank number" + iffConversionConfig.fileMaxBlank)
              }
            } else {
              //. 检查记录的列数
              if (countLineNumber == 0 ) {
                var lineSeq:Int = StringUtils.splitByWholeSeparatorPreserveAllTokens(lineStr,iffMetadata.srcSeparator).length
                if(lineStr.endsWith(iffMetadata.srcSeparator)){
                  lineSeq -= 1;
                }
                if(lineSeq!= iffMetadata.body.getSourceLength) {
                  logger.error("file " + iffConversionConfig.filename + " record column error lineStr:" + lineSeq + " iffMetadata.body" + iffMetadata.body.getSourceLength, "file number is not right")
                  throw RecordNotFixedException("file " + iffConversionConfig.filename + " record column number is not right,Expect record column number:"+ iffMetadata.body.getSourceLength+" Actually record column number:" + lineSeq)
                }
              }
              //换行符号
              currentLineLength = lineStr.getBytes(this.iffMetadata.sourceCharset).length+iffConversionConfig.lengthOfLineEnd
             // logger.info("block Info", "第" + blockIndex + "块" + currentLineLength+" data:="+lineStr)
              if (endOfFile || (currentBlockReadBytesCount + currentLineLength) > blockSize) {
                canRead = false
              } else {
                currentBlockReadBytesCount += currentLineLength
              }
              countLineNumber += 1
            }
          }
        }else {
          endOfFile = true
          canRead = false
        }
      }
      val blockPosition: (Int, Long, Int) = (blockIndex, totalBlockReadBytesCount, currentBlockReadBytesCount)
      blockPositionQueue.put(blockPosition)
      totalBlockReadBytesCount += currentBlockReadBytesCount
      blockIndex += 1
    }
    logger.info("blockPositionQueue Info", "blockPositionQueueSize:"+blockPositionQueue.size())
    try{
      br.close()
    }catch {
      case e:Exception=>
        e.printStackTrace()
        logger.error("bufferedReader close error","bufferedReader close error")
    }
    try{
      iffFileInputStream.close()
    }catch {
      case e:Exception=>
        e.printStackTrace()
        logger.error("iffFileInputStream close error","iffFileInputStream close error")
    }
    blockPositionQueue
  }
  protected def getDataFileProcessor():DataFileProcessor={
    new UnfixDataFileProcessor
  }
}
/**
  *  Spark 程序入口
  *  @author www.birdiexx.com
  */
object MutilUnfixedConversionOnSpark extends App{
  val config = new MutilConversionOnSparkConfig()
  val job = new MutilUnfixedConversionOnSparkJob()
  val logger = job.logger
  try {
    job.start(config, args)
  } catch {
    case t:RecordNumberErrorException =>
      t.printStackTrace()
      System.exit(1)
    case t:MaxErrorNumberException =>
      t.printStackTrace()
      System.exit(2)
    case t:RecordNotFixedException =>
      t.printStackTrace()
      System.exit(3)
    case t:MaxBlankNumberException =>
      t.printStackTrace()
      System.exit(4)
    case t: Throwable =>
      t.printStackTrace()
      if(StringUtils.isNotEmpty(t.getMessage)) logger.error(MESSAGE_ID_CNV1001, t.getMessage)
      System.exit(9)
  }
}
