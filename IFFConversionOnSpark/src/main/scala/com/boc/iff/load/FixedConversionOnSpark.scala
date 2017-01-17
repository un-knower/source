package com.boc.iff.load


import com.boc.iff.IFFConversion._
import com.boc.iff.exception.{MaxErrorNumberException, RecordNumberErrorException}
import org.apache.commons.lang3.StringUtils

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent._

/**
  * @author www.birdiexx.com
  */
class FixedConversionOnSparkJob
  extends BaseConversionOnSparkJob[BaseConversionOnSparkConfig] {

  protected def createBlockPositionQueue(filePath:String,conversionJob: ((Int, Long, Int,String)=>String)):String = {
    //块大小至少要等于数据行大小
    val blockSize = math.max(iffConversionConfig.blockSize, iffFileInfo.recordLength + 1)
    logger.info("recordLength","recordLength"+iffFileInfo.recordLength)
    val recordBuffer = new Array[Byte](iffFileInfo.recordLength + iffConversionConfig.lengthOfLineEnd) //把换行符号也读入到缓冲byte
    var totalBlockReadBytesCount: Long = 0
    val iffFileInputStream = openIFFFileBufferedInputStream(filePath, fileGzipFlgMap(filePath), iffConversionConfig.readBufferSize)
    var blockIndex: Int = 0
    var endOfFile = false
    while (!endOfFile) {
      var currentBlockReadBytesCount: Int = 0
      var canRead = true
      while (canRead) {
        val length = iffFileInputStream.read(recordBuffer)
        if (length != -1) {
          currentBlockReadBytesCount += recordBuffer.length
          //当文件读完，或者已读取一个块大小的数据（若再读一行则超过块大小）的时候，跳出循环
        } else {
          endOfFile = true
        }
        if (endOfFile || blockSize - currentBlockReadBytesCount < recordBuffer.length) {
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
      iffFileInputStream.close()
    }catch {
      case e:Exception=>
        e.printStackTrace()
        logger.error("iffFileInputStream close error","iffFileInputStream close error")
    }
    filePath
  }

  override protected def getDataFileProcessor(): DataFileProcessor = new FixDataFileProcessor
}

/**
  * Spark 程序入口
  *  @author www.birdiexx.com
  */
object FixedConversionOnSpark extends App {
  val config = new BaseConversionOnSparkConfig()
  val job = new FixedConversionOnSparkJob()
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
