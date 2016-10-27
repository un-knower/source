package com.boc.iff

import java.io.{BufferedReader, FileInputStream, InputStreamReader}
import java.util.Properties
import java.util.concurrent.LinkedBlockingQueue
import java.util.zip.GZIPInputStream

import com.boc.iff.DFSUtils.FileMode
import com.boc.iff.IFFConversion._
import com.boc.iff.model._
import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.yarn.conf.YarnConfiguration
import com.boc.iff.exception._

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.concurrent.duration.Duration
import scala.concurrent._
import ExecutionContext.Implicits.global

/**
  * @author www.birdiexx.com
  */
class UnfixedConversionOnSparkJob
  extends BaseConversionOnSparkJob[BaseConversionOnSparkConfig] {

  protected def createBlockPositionQueue: java.util.concurrent.LinkedBlockingQueue[(Int, Long, Int)] = {
    //块大小至少要等于数据行大小
    val blockSize = math.max(iffConversionConfig.blockSize, 0)
    logger.info("blockSize","blockSize:"+blockSize)
    val blockPositionQueue = new LinkedBlockingQueue[(Int, Long, Int)]()
    var totalBlockReadBytesCount: Long = 0
    val iffFileInputStream = openIFFFileInputStream(iffConversionConfig.iffFileInputPath+iffConversionConfig.filename)
    var blockIndex: Int = 0
    var endOfFile = false
    val br = new BufferedReader(new InputStreamReader(iffFileInputStream.asInstanceOf[java.io.InputStream],IFFUtils.getCharset(iffMetadata.sourceCharset)))
    var currentLineLength: Int = 0
    var countLineNumber: Int = 0
    var blankLineNumber: Int = 0
    val needCheckBlank: Boolean = if(iffConversionConfig.fileMaxBlank==0) false else true

    logger.info("chartSet","chartSet"+iffMetadata.sourceCharset)
    val validateRecNumFlag = if("Y".equals(iffConversionConfig.validateRecNumFlag))true else false
    while (!endOfFile) {
      var currentBlockReadBytesCount: Int = 0
      var canRead = true
      currentBlockReadBytesCount += currentLineLength
      while (canRead) {
        val lineStr = br.readLine()
        if(lineStr!=null){
          if(validateRecNumFlag&&lineStr.startsWith(iffConversionConfig.fileEOFPrefix)){
            if(lineStr.startsWith(iffConversionConfig.fileEOFPrefix+"RecNum")){
              var recNum = lineStr.substring((iffConversionConfig.fileEOFPrefix+"RecNum=").length)
              if(StringUtils.isNotEmpty(recNum))recNum=recNum.trim
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

  /**
    * 创建一个方法 对一个分片（分区）的数据进行转换操作
    *
    * @return
    */
  protected def createConvertOnDFSByPartitionsFunction: (Iterator[(Int, Long, Int)] => Iterator[String]) = {
    val iffConversionConfig = this.iffConversionConfig
    val iffMetadata = this.iffMetadata
    val iffFileInfo = this.iffFileInfo
    val fieldDelimiter = this.fieldDelimiter
    val lineSplit = iffMetadata.srcSeparator
    val lengthOfLineEnd = iffConversionConfig.lengthOfLineEnd
    implicit val configuration = sparkContext.hadoopConfiguration
    val hadoopConfigurationMap = mutable.HashMap[String,String]()
    val iterator = configuration.iterator()
    val prop = new Properties()
    prop.load(new FileInputStream(iffConversionConfig.configFilePath))
    while (iterator.hasNext){
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
      val convertField: (IFFField, mutable.HashMap[String,Any])=> String = { (iffField, record) =>
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
      for((key,value)<-hadoopConfigurationMap){
        configuration.set(key, value)
      }
      val fileSystem = FileSystem.get(configuration)
      val iffFileInputPath = new Path(iffFileInputPathText)
      val iffFileInputStream = fileSystem.open(iffFileInputPath)
      val iffFileSourceInputStream =
        if(iffFileInfo.isGzip) new GZIPInputStream(iffFileInputStream, readBufferSize)
        else iffFileInputStream
      val inputStream = iffFileSourceInputStream.asInstanceOf[java.io.InputStream]
      logger.info("blockPositionIterator:","blockPositionIterator"+charset)
      while(blockPositionIterator.hasNext){
        val (blockIndex, blockPosition, blockSize) = blockPositionIterator.next()
        var currentBlockReadBytesCount: Long = 0
        var restToSkip = blockPosition
        while (restToSkip > 0) {
          val skipped = inputStream.skip(restToSkip)
          restToSkip = restToSkip - skipped
        }
        val br = new BufferedReader(new InputStreamReader(inputStream,charset))
        while ( currentBlockReadBytesCount < blockSize ) {
          val currentLine = br.readLine()

          if (StringUtils.isNotEmpty(currentLine.trim)) {

            // logger.info("currentLine:","currentLine"+currentLine)
            val recordLength = currentLine.getBytes(iffMetadata.sourceCharset).length
            currentBlockReadBytesCount += recordLength + lengthOfLineEnd
            val lineSeq = StringUtils.splitByWholeSeparatorPreserveAllTokens(currentLine, lineSplit)

            var dataInd = 0
            val dataMap = new mutable.HashMap[String, Any]
            val sb = new mutable.StringBuilder(recordLength)
            var success = true
            var errorMessage = "";
            try {
              for (iffField <- iffMetadata.body.fields if ("Y".equals(iffField.virtual))) {
                val fieldType = iffField.typeInfo
                if(StringUtils.isNotBlank(lineSeq(dataInd))) {
                  fieldType match {
                    case fieldType: CInteger => dataMap += (iffField.name -> lineSeq(dataInd).toInt)
                    case fieldType: CDecimal => dataMap += (iffField.name -> lineSeq(dataInd).toDouble)
                    case _ => dataMap += (iffField.name -> lineSeq(dataInd))
                  }
                }else{
                  dataMap += (iffField.name -> "")
                }

                dataInd += 1
              }
            } catch {
              case e: NumberFormatException =>
                success = false
                errorMessage = " String to Number Exception "
              case e: Exception =>
                success = false
                errorMessage = " unknown exception " + e.getMessage
            }
            if (success) {
              import com.boc.iff.CommonFieldValidatorContext._
              implicit val validContext = new CommonFieldValidatorContext
              for (iffField <- iffMetadata.body.fields if success && !iffField.isFilter) {
                try {
                  sb ++= convertField(iffField, dataMap) //调用上面定义的闭包方法转换一个字段的数据
                  sb ++= fieldDelimiter
                  success = if (iffField.validateField(dataMap)) true else false
                  errorMessage = if (!success) iffField.name + "ERROR validateField" else ""
                }catch{
                  case e:Exception=>
                    success = false
                    errorMessage = iffField.name+" "+e.getMessage
                }
              }
            }
            if (!success) {
              sb.setLength(0)
              sb.append(currentLine).append(lineSplit).append(errorMessage).append("ERROR")
              logger.error("ERROR validateField", currentLine)
            }
            recordList += sb.toString
          }
        }
        try{
          br.close()
        }catch {
          case e:Exception=>
            e.printStackTrace()
            logger.error("bufferedReader close error","bufferedReader close error")
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
  *  Spark 程序入口
  *  @author www.birdiexx.com
  */
object UnfixedConversionOnSpark extends App{
  val config = new BaseConversionOnSparkConfig()
  val job = new UnfixedConversionOnSparkJob()
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
