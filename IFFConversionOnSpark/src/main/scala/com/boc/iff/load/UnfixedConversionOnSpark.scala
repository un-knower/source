package com.boc.iff.load

import java.io.{BufferedReader, InputStreamReader}

import com.boc.iff.IFFConversion._
import com.boc.iff.exception._
import com.boc.iff.IFFUtils
import org.apache.commons.lang3.StringUtils

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent._

/**
  * @author www.birdiexx.com
  */
class UnfixedConversionOnSparkJob
  extends BaseConversionOnSparkJob[BaseConversionOnSparkConfig] {

  protected def createBlockPositionQueue(filePath:String,conversionJob: ((Int, Long, Int,String)=>String)):String = {
    //块大小至少要等于数据行大小
    val blockSize = math.max(iffConversionConfig.blockSize, 0)
    var totalBlockReadBytesCount: Long = 0
    val iffFileInputStream = openIFFFileBufferedInputStream(filePath, fileGzipFlgMap(filePath), iffConversionConfig.readBufferSize)
    var blockIndex: Int = 0
    var endOfFile = false
    val br = new BufferedReader(new InputStreamReader(iffFileInputStream.asInstanceOf[java.io.InputStream],IFFUtils.getCharset(iffMetadata.sourceCharset)))
    var currentLineLength: Int = 0
    var countLineNumber: Int = 0
    var firstRow: Boolean =  true

    while (!endOfFile) {
      var currentBlockReadBytesCount: Int = 0
      var canRead = true
      currentBlockReadBytesCount += currentLineLength
      while (canRead) {
        val lineStr = br.readLine()
        if(lineStr!=null){
          if(!lineStr.startsWith(iffConversionConfig.fileEOFPrefix)){
            //. 检查记录的列数
            if (firstRow) {
              firstRow =  false
              var lineSeq:Int = StringUtils.countMatches(lineStr,iffMetadata.srcSeparator)
              if("N".endsWith(iffConversionConfig.dataLineEndWithSeparatorF)){
                lineSeq+=1
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
        }else {
          endOfFile = true
          canRead = false
        }
      }

      val bIndex = blockIndex
      val totalSize = totalBlockReadBytesCount
      val currentBlocksSize = currentBlockReadBytesCount
      val conversionFuture = future { conversionJob(bIndex, totalSize, currentBlocksSize,filePath) }
      this.futureQueue.put(conversionFuture)
      totalBlockReadBytesCount += currentBlockReadBytesCount
      blockIndex += 1
    }
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
    filePath
  }

  protected def getDataFileProcessor():DataFileProcessor={
    new UnfixDataFileProcessor
  }
}
/**
  *  Spark 程序入口
  *  @author www.birdiexx.com
  */
object UnfixedConversionOnSpark extends App{
  val config = new BaseConversionOnSparkConfig()
  val job = new UnfixedConversionOnSparkJob() with ParquetConversionOnSparkJob[BaseConversionOnSparkConfig]
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
