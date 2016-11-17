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
class FixedConversionOnSparkJob
  extends BaseConversionOnSparkJob[BaseConversionOnSparkConfig] {

  protected def createBlockPositionQueue: java.util.concurrent.LinkedBlockingQueue[(Int, Long, Int)] = {
    //块大小至少要等于数据行大小
    val blockSize = math.max(iffConversionConfig.blockSize, iffFileInfo.recordLength + 1)
    val blockPositionQueue = new LinkedBlockingQueue[(Int, Long, Int)]()
    logger.info("recordLength","recordLength"+iffFileInfo.recordLength)
    val recordBuffer = new Array[Byte](iffFileInfo.recordLength + iffConversionConfig.lengthOfLineEnd) //把换行符号也读入到缓冲byte
    var totalBlockReadBytesCount: Long = 0
    val iffFileInputStream = openIFFFileBufferedInputStream(
      iffConversionConfig.iffFileInputPath, iffFileInfo.isGzip, iffConversionConfig.readBufferSize)
    var blockIndex: Int = 0
    var endOfFile = false
    var countLineNumber: Int = 0
    val endOfFileStr = new StringBuffer()
    var recordEnd = false
    val validateRecNumFlag = if("Y".equals(iffConversionConfig.validateRecNumFlag))true else false
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
    if (validateRecNumFlag&&endOfFileStr.length() > 0) {
      val lineSeq = StringUtils.splitByWholeSeparatorPreserveAllTokens(endOfFileStr.toString, iffConversionConfig.fileEOFPrefix)
      for (s <- lineSeq) {
        if (s.startsWith("RecNum")) {
          var recNum = s.substring(("RecNum=").length, s.length - (iffConversionConfig.lengthOfLineEnd))
          if(StringUtils.isNotEmpty(recNum))recNum=recNum.trim
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


  /**
    * 创建一个方法 对一个分片（分区）的数据进行转换操作
    *@author www.birdiexx.com
    * @return
    */
  protected def createConvertOnDFSByPartitionsFunction: (Iterator[(Int, Long, Int)] => Iterator[String]) = {
    val iffConversionConfig = this.iffConversionConfig
    val lengthOfLineEnd = iffConversionConfig.lengthOfLineEnd
    val iffMetadata = this.iffMetadata
    val iffFileInfo = this.iffFileInfo
    val fieldDelimiter = this.fieldDelimiter
    val specialCharConvertor = this.specialCharConvertor
    val needConvertSpecialChar:Boolean = if("Y".equals(this.iffConversionConfig.specialCharConvertFlag))true else false
    implicit val configuration = sparkContext.hadoopConfiguration
    val hadoopConfigurationMap = mutable.HashMap[String, String]()
    val iterator = configuration.iterator()
    val prop = new Properties()
    prop.load(new FileInputStream(iffConversionConfig.configFilePath))
    while (iterator.hasNext) {
      val entry = iterator.next()
      hadoopConfigurationMap += entry.getKey -> entry.getValue
    }
    val iffFileInputPathText = iffConversionConfig.iffFileInputPath
    val readBufferSize = iffConversionConfig.readBufferSize

    val convertByPartitionsFunction: (Iterator[(Int, Long, Int)] => Iterator[String]) = { blockPositionIterator =>
      val logger = new ECCLogger()
      logger.configure(prop)
      val recordList = ListBuffer[String]()

      import com.boc.iff.CommonFieldConvertorContext._
      val charset = IFFUtils.getCharset(iffMetadata.sourceCharset)
      val decoder = charset.newDecoder
      implicit val convertorContext = new CommonFieldConvertorContext(iffMetadata, iffFileInfo, decoder)

      /*
        对一个字段的数据进行转换操作
        为了减少层次，提高程序可读性，这里定义了一个闭包方法作为参数，会在下面的 while 循环中被调用
       */
      val convertField: (IFFField, mutable.HashMap[String, Any]) => String = { (iffField, record) =>
        if (iffField.isFilter) ""
        else if (iffField.isConstant) {
          iffField.getDefaultValue.replaceAll("#FILENAME#", iffFileInfo.fileName)
        } else {
          try {
            /*
              通过一些隐式转换对象和方法，使 IFFField 对象看起来像拥有了 convert 方法一样
              化被动为主动，可使程序语义逻辑更清晰
             */
            iffField.convert(record)
          } catch {
            case e: Exception =>
              logger.error(MESSAGE_ID_CNV1001, "invaild record found in : " + iffField.getName)
              logger.error(MESSAGE_ID_CNV1001, "invaild record found data : " + record)
              ""
          }
        }
      }

      val configuration = new YarnConfiguration()
      for ((key, value) <- hadoopConfigurationMap) {
        configuration.set(key, value)
      }
      val fileSystem = FileSystem.get(configuration)
      val iffFileInputPath = new Path(iffFileInputPathText)
      val iffFileInputStream = fileSystem.open(iffFileInputPath)
      val iffFileSourceInputStream =
        if (iffFileInfo.isGzip) new GZIPInputStream(iffFileInputStream, readBufferSize)
        else iffFileInputStream

      while (blockPositionIterator.hasNext) {
        val (blockIndex, blockPosition, blockSize) = blockPositionIterator.next()
        var currentBlockReadBytesCount: Long = 0
        var restToSkip = blockPosition
        while (restToSkip > 0) {
          val skipped = iffFileSourceInputStream.skip(restToSkip)
          restToSkip = restToSkip - skipped
        }
        val recordLen = iffFileInfo.recordLength + lengthOfLineEnd
        val recordBytes = new Array[Byte](recordLen)
        while (currentBlockReadBytesCount < blockSize) {
          var recordLength: Int = 0
          while (recordLength < recordLen) {
            val readLength = iffFileSourceInputStream.read(
              recordBytes, recordLength, recordLen - recordLength)
            if (readLength == -1) {
              recordLength = recordLen
            } else {
              recordLength += readLength
            }
          }


         // logger.info("currentRec", "currentRec:" + new String(recordBytes, iffMetadata.sourceCharset))
          val dataMap = new mutable.HashMap[String, Any]
          var success = true
          var errorMessage = ""
          var fname=""
          try {
            for (iffField <- iffMetadata.body.fields if (!iffField.virtual)) {
              fname = iffField.name
              var fieldVal = new String(java.util.Arrays.copyOfRange(recordBytes, iffField.startPos, iffField.endPos + 1), iffMetadata.sourceCharset)
              val fieldType = iffField.typeInfo
              if(StringUtils.isNotBlank(fieldVal)) {
                if(needConvertSpecialChar){
                  fieldVal = specialCharConvertor.convert(fieldVal)
                }
                fieldType match {
                  case fieldType: CInteger => dataMap += (iffField.name -> fieldVal.toInt)
                  case fieldType: CDecimal => dataMap += (iffField.name -> fieldVal.toDouble)
                  case _ => dataMap += (iffField.name -> fieldVal)
                }
              }else{
                dataMap += (iffField.name -> "")
              }
            }
          } catch {
            case e: NumberFormatException =>
              success = false
              errorMessage = fname+" String to Number Exception "
            case e: Exception =>
              success = false
              errorMessage = fname+" unknown exception " + e.getMessage
          }
          val sb = new mutable.StringBuilder(recordBytes.length)
          import com.boc.iff.CommonFieldValidatorContext._
          implicit val validContext = new CommonFieldValidatorContext
          for (iffField <- iffMetadata.body.fields if success) {
            try {
              sb ++= convertField(iffField, dataMap) //调用上面定义的闭包方法转换一个字段的数据
              sb ++= fieldDelimiter
              success = if (iffField.validateField(dataMap)) true else false
              errorMessage = if (!success) "ERROR validateField" else ""
            }catch{
              case e:Exception=>
                success = false
                errorMessage = iffField.name+" "+e.getMessage
            }
          }
          if (!success) {
            sb.setLength(0)
            sb.append(new String(recordBytes, iffMetadata.sourceCharset)).append(errorMessage).append("ERROR")
            logger.error("ERROR validateField", new String(recordBytes, iffMetadata.sourceCharset))
          }
          recordList += sb.toString
          currentBlockReadBytesCount += recordLength
        }
      }
      try{
        iffFileSourceInputStream.close()
      }catch {
        case e:Exception=>
          e.printStackTrace()
          logger.error("iffFileInputStream close error","iffFileInputStream close error")
      }
      recordList.iterator
    }
    convertByPartitionsFunction
  }

}



/**
  * Spark 程序入口
  * @author www.birdiexx.com
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

/**
  * @author www.birdiexx.com
  */
