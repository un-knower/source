package com.boc.iff.load

import java.io.FileInputStream
import java.util.Properties
import java.util.concurrent.LinkedBlockingQueue
import java.util.zip.GZIPInputStream

import com.boc.iff.IFFConversion._
import com.boc.iff.exception.{MaxErrorNumberException, RecordNumberErrorException}
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
class MutilFixedConversionOnSparkJob
  extends MutilConversionOnSparkJob[MutilConversionOnSparkConfig] {

  protected def createBlockPositionQueue(filePath:String): java.util.concurrent.LinkedBlockingQueue[(Int, Long, Int)] = {
    //块大小至少要等于数据行大小
    val blockSize = math.max(iffConversionConfig.blockSize, iffFileInfo.recordLength + 1)
    val blockPositionQueue = new LinkedBlockingQueue[(Int, Long, Int)]()
    logger.info("recordLength","recordLength"+iffFileInfo.recordLength)
    val recordBuffer = new Array[Byte](iffFileInfo.recordLength + iffConversionConfig.lengthOfLineEnd) //把换行符号也读入到缓冲byte
    var totalBlockReadBytesCount: Long = 0
    val iffFileInputStream = openIFFFileBufferedInputStream(filePath, fileGzipFlgMap(filePath), iffConversionConfig.readBufferSize)
    var blockIndex: Int = 0
    var endOfFile = false
    var countLineNumber: Int = 0
    val endOfFileStr = new StringBuffer()
    var recordEnd = false
    while (!endOfFile) {
      var currentBlockReadBytesCount: Int = 0
      var canRead = true
      while (canRead) {
        val length = iffFileInputStream.read(recordBuffer)
        if (length != -1) {
          val lineStr = new String(recordBuffer, iffMetadata.sourceCharset)
          //logger.info("lineStr","lineStr:"+lineStr)
          if (lineStr.startsWith(iffConversionConfig.fileEOFPrefix) || recordEnd) {
            recordEnd = true
            endOfFileStr.append(lineStr)
          } else {
            currentBlockReadBytesCount += recordBuffer.length
            countLineNumber += 1
          }
          //当文件读完，或者已读取一个块大小的数据（若再读一行则超过块大小）的时候，跳出循环
        } else {
          endOfFile = true
        }
        if (endOfFile || blockSize - currentBlockReadBytesCount < recordBuffer.length) {
          canRead = false
        }
      }
      val blockPosition: (Int, Long, Int) = (blockIndex, totalBlockReadBytesCount, currentBlockReadBytesCount)
      logger.debug(MESSAGE_ID_CNV1001,
        "Block Index: %-5d, Position: %-10d, Size: %-10d".format(blockPosition._1, blockPosition._2, blockPosition._3))
      blockPositionQueue.put(blockPosition)
      totalBlockReadBytesCount += currentBlockReadBytesCount
      blockIndex += 1
    }
    if (endOfFileStr.length() > 0) {
      println(endOfFileStr.toString)
      val lineSeq = StringUtils.splitByWholeSeparatorPreserveAllTokens(endOfFileStr.toString, iffConversionConfig.fileEOFPrefix)
      for (s <- lineSeq) {
        if (s.startsWith("RecNum")) {
          val recNum = s.substring(("RecNum=").length, s.length - (iffConversionConfig.lengthOfLineEnd))
          if (recNum.toInt != countLineNumber) {
            logger.error("file " + iffConversionConfig.filename + " number is not right " + recNum.toInt + countLineNumber, "file number is not right")
            throw RecordNumberErrorException("file " + iffConversionConfig.filename + " record number is not right,Expect record number:"+ countLineNumber+" Actually record number:" + recNum.toInt )
          }
        }
      }
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

  override protected def getDataFileProcessor(): DataFileProcessor = new FixDataFileProcessor
}

/**
  * Spark 程序入口
  *  @author www.birdiexx.com
  */
object MutilFixedConversionOnSpark extends App {
  val config = new MutilConversionOnSparkConfig()
  val job = new MutilFixedConversionOnSparkJob()
  val logger = job.logger
  try {
    job.start(config, args)
  } catch {
    case t: RecordNumberErrorException =>
      t.printStackTrace()
      System.exit(1)
    case t:MaxErrorNumberException =>
      t.printStackTrace()
      System.exit(2)
    case t: Throwable =>
      t.printStackTrace()
      if (StringUtils.isNotEmpty(t.getMessage)) logger.error(MESSAGE_ID_CNV1001, t.getMessage)
      System.exit(9)
  }
}
