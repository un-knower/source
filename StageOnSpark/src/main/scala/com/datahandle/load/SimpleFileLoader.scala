package com.datahandle.load

import java.io._
import java.util
import java.util.Properties
import java.util.concurrent.LinkedBlockingQueue
import java.util.zip.GZIPInputStream

import com.boc.iff._
import com.boc.iff.exception.RecordNotFixedException
import com.boc.iff.model.{CDecimal, CInteger, IFFField, IFFFileInfo}
import com.model.FileInfo.FileType
import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent._
import scala.concurrent.duration.Duration

/**
  * Created by scutlxj on 2017/1/22.
  * 加载原始文件
  */
class SimpleFileLoader extends FileLoader{

  val readBufferSize: Int = 8 * 1024 * 1024                                 //文件读取缓冲区大小, 默认 8M

  var iffFileInfo:IFFFileInfo = _
  val fileGzipFlgMap = new mutable.HashMap[String,Boolean]
  val fileInMap = new mutable.HashMap[String,String]
  protected val standbyFileQueue = new LinkedBlockingQueue[String]()
  protected val processFileQueue = new LinkedBlockingQueue[String]()
  protected val futureQueue = new LinkedBlockingQueue[Future[RDD[String]]]()
  protected val rddQueue = new LinkedBlockingQueue[RDD[String]]()

  def loadFile(): DataFrame = {
    val convertByPartitions = createConvertByPartitionsFunction
    val conversionJob: ((Int, Long, Int, String) => RDD[String]) = { (blockIndex, blockPosition, blockSize, filePath) =>
      val rdd = this.sparkContext.makeRDD(Seq((blockIndex, blockPosition, blockSize, filePath)), 1)
      rdd.mapPartitions(convertByPartitions)

    }
    createBlocks(conversionJob)
    var targetRdd = rddQueue.take()
    while(!rddQueue.isEmpty){
      targetRdd = targetRdd.union(rddQueue.take())
    }
    targetRdd.cache()
    val errorPath = getErrorPath()
    implicit val configuration: Configuration = sparkContext.hadoopConfiguration
    DFSUtils.deleteDir(errorPath)
    targetRdd.filter(_.endsWith("ERROR")).saveAsTextFile(errorPath)
    val rdd = targetRdd.filter(x=>(!x.endsWith("ERROR")))
    targetRdd.persist()
    changeRddToDataFrame(rdd)
  }

  /**
    * 解析 IFF 数据文件结构
    *
    * @param iffFileName     IFF 文件路径
    * @param readBufferSize  读取缓冲区大小
    */
  protected def loadIFFFileInfo(iffFileName: String, readBufferSize: Int): Unit ={
    iffFileInfo = new IFFFileInfo()
    iffFileInfo.recordLength = tableInfo.header.getLength
    iffFileInfo.recordLength = math.max(iffFileInfo.recordLength, tableInfo.body.getLength)
    iffFileInfo.recordLength = math.max(iffFileInfo.recordLength, tableInfo.footer.getLength)
    if(tableInfo.fixedLength!=null) {
      iffFileInfo.recordLength = math.max(iffFileInfo.recordLength, tableInfo.fixedLength.toInt)
    }
    if(fileInfo.dataPath.endsWith("/"))fileInfo.dataPath=fileInfo.dataPath.substring(0,fileInfo.dataPath.lastIndexOf("/"))
    iffFileInfo.fileName = fileInfo.dataPath.substring(fileInfo.dataPath.lastIndexOf("/")+1)
  }

  private def createBlocks(conversionJob: ((Int, Long, Int,String)=>RDD[String])): Unit = {
    getIFFFilePath(fileInfo.dataPath)
    while(this.standbyFileQueue.size()>0) {
      val filePath = standbyFileQueue.take()
      fileInfo.fileType match {
        case FileType.FIXLENGTH => createBlockPositionQueueFix(filePath,conversionJob)
        case _ => createBlockPositionQueueUnfix(filePath,conversionJob)
      }
    }
    while(!futureQueue.isEmpty){
      val future = futureQueue.take()
      val rdd = Await.result(future, Duration.Inf)
      rddQueue.put(rdd)
    }

  }

  protected def createBlockPositionQueueUnfix(filePath:String,conversionJob: ((Int, Long, Int,String)=>RDD[String])):String = {
    //块大小至少要等于数据行大小
    val blockSize = math.max(jobConfig.blockSize, 0)
    var totalBlockReadBytesCount: Long = 0
    val iffFileInputStream = openIFFFileBufferedInputStream(filePath, fileGzipFlgMap(filePath), this.readBufferSize)
    var blockIndex: Int = 0
    var endOfFile = false
    val br = new BufferedReader(new InputStreamReader(iffFileInputStream.asInstanceOf[java.io.InputStream],IFFUtils.getCharset(tableInfo.sourceCharset)))
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
          if(!lineStr.startsWith(tableInfo.fileEOFPrefix)){
            //. 检查记录的列数
            if (firstRow) {
              firstRow =  false
              var lineSeq:Int = StringUtils.countMatches(lineStr,tableInfo.srcSeparator)
              if(!tableInfo.dataLineEndWithSeparatorF){
                lineSeq+=1
              }
              if(lineSeq!= tableInfo.body.getSourceLength) {
               // logger.error("file " + iffConversionConfig.filename + " record column error lineStr:" + lineSeq + " iffMetadata.body" + iffMetadata.body.getSourceLength, "file number is not right")
                throw RecordNotFixedException("file " + fileInfo.dataPath + " record column number is not right,Expect record column number:"+ tableInfo.body.getSourceLength+" Actually record column number:" + lineSeq)
              }
            }
            //换行符号
            currentLineLength = lineStr.getBytes(tableInfo.sourceCharset).length+tableInfo.lengthOfLineEnd
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
        //logger.error("bufferedReader close error","bufferedReader close error")
    }
    try{
      iffFileInputStream.close()
    }catch {
      case e:Exception=>
        e.printStackTrace()
        //logger.error("iffFileInputStream close error","iffFileInputStream close error")
    }
    filePath
  }

  protected def createBlockPositionQueueFix(filePath:String,conversionJob: ((Int, Long, Int,String)=>RDD[String])):String = {
    //块大小至少要等于数据行大小
    val blockSize = math.max(jobConfig.blockSize, iffFileInfo.recordLength + tableInfo.lengthOfLineEnd)
    //logger.info("recordLength","recordLength"+iffFileInfo.recordLength)
    val recordBuffer = new Array[Byte](iffFileInfo.recordLength + tableInfo.lengthOfLineEnd) //把换行符号也读入到缓冲byte
    var totalBlockReadBytesCount: Long = 0
    val iffFileInputStream = openIFFFileBufferedInputStream(filePath, fileGzipFlgMap(filePath), readBufferSize)
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
        //logger.error("iffFileInputStream close error","iffFileInputStream close error")
    }
    filePath
  }

  private def getIFFFilePath(fileName: String):Unit = {
    val fileSystem = FileSystem.get(sparkContext.hadoopConfiguration)
    val filePath = new Path(fileName)
    if (fileSystem.isDirectory(filePath)) {
      val fileStatus = fileSystem.listStatus(filePath)
      for (fs <- fileStatus) {
        fileGzipFlgMap += (fs.getPath.toString -> checkGZipFile(fs.getPath.toString))
        standbyFileQueue.put(fileName)
      }
    } else {
      standbyFileQueue.put(fileName)
      fileGzipFlgMap += (fileName -> checkGZipFile(fileName))
    }
  }

  /**
    * 识别文件是否 GZip 格式
    *
    * @param fileName 文件路径
    * @return
    */
  private def checkGZipFile(fileName: String): Boolean = {
    //val zipIndicator = Array[Byte](80, 75, 0x3, 0x4)
    val gzipIndicator = Array[Byte](31, -117)
    val fileInputStream = openIFFFileInputStream(fileName)
    var bufferedInputStream = new BufferedInputStream(fileInputStream, gzipIndicator.length)
    var isGZip= false
    val magicHeader: Array[Byte] = new Array[Byte](gzipIndicator.length)
    bufferedInputStream.read(magicHeader, 0, gzipIndicator.length)
    if (java.util.Arrays.equals(magicHeader, gzipIndicator)) {
      isGZip = true
    }
    bufferedInputStream.close()
    bufferedInputStream = null
    isGZip
  }

  private def openIFFFileInputStream(fileName: String): java.io.InputStream = {
    val fileSystem = FileSystem.get(sparkContext.hadoopConfiguration)
    val filePath = new Path(fileName)
    val iffFileInputStream = fileSystem.open(filePath, readBufferSize)
    iffFileInputStream
  }

  /**
    * 打开本地文件流
    *
    * @param fileName 文件路径
    * @param isGZip   文件是否 GZip 格式
    * @param readBufferSize 读取缓冲区大小
    * @return
    */
  protected def openIFFFileBufferedInputStream(fileName: String,
                                               isGZip: Boolean,
                                               readBufferSize: Int): BufferedInputStream = {
    val iffFileInputStream = openIFFFileInputStream(fileName)
    val iffFileSourceStream =
      isGZip match {
        case true =>
          new GZIPInputStream(iffFileInputStream)
        case _ =>
          iffFileInputStream
      }
    new BufferedInputStream(iffFileSourceStream, readBufferSize)
  }

  /**
    * 创建一个方法 对一个分片（分区）的数据进行转换操作
    *
    * @return
    */
  protected def createConvertByPartitionsFunction: (Iterator[(Int, Long, Int,String)] => Iterator[String]) = {
   // val iffConversionConfig = this.iffConversionConfig
    val tableInfo = this.tableInfo
    val iffFileInfo = this.iffFileInfo
    val lengthOfLineEnd = tableInfo.lengthOfLineEnd
    val fieldDelimiter = this.fieldDelimiter
    val lineSplit = tableInfo.srcSeparator
    val fileGzipFlgMap = this.fileGzipFlgMap
    implicit val configuration = sparkContext.hadoopConfiguration
    val hadoopConfigurationMap = mutable.HashMap[String,String]()
    val iterator = configuration.iterator()
    while (iterator.hasNext){
      val entry = iterator.next()
      hadoopConfigurationMap += entry.getKey -> entry.getValue
    }
    val readBufferSize = this.readBufferSize

    val dataFileProcessor = this.getDataFileProcessor()
    dataFileProcessor.iffMetadata = tableInfo
    dataFileProcessor.lengthOfLineEnd = lengthOfLineEnd
    dataFileProcessor.iFFFileInfo = iffFileInfo

    val pro = new Properties
    pro.load(new FileInputStream(stageAppContext.jobConfig.configPath))



    val convertByPartitionsFunction: (Iterator[(Int, Long, Int,String)] => Iterator[String]) = { blockPositionIterator =>
      val recordList = ListBuffer[String]()
      val charset = IFFUtils.getCharset(tableInfo.sourceCharset)
      dataFileProcessor.charset = charset
      val decoder = charset.newDecoder

      /*
        对一个字段的数据进行转换操作
        为了减少层次，提高程序可读性，这里定义了一个闭包方法作为参数，会在下面的 while 循环中被调用
       */
      val convertField: (IFFField, java.util.HashMap[String,Any])=> String = { (iffField, record) =>
        if (iffField.isFilter) ""
        else if (iffField.isConstant) {
          iffField.getDefaultValue.replaceAll("#FILENAME#", iffFileInfo.fileName)
        } else {
          import com.boc.iff.CommonFieldConvertorContext._
          implicit val convertorContext = new CommonFieldConvertorContext(tableInfo, iffFileInfo, decoder)
          try {
            /*
              通过一些隐式转换对象和方法，使 IFFField 对象看起来像拥有了 convert 方法一样
              化被动为主动，可使程序语义逻辑更清晰
             */
            iffField.convert(record)
          } catch {
            case e: Exception =>
              //logger.error(MESSAGE_ID_CNV1001, "invaild record found in : " + iffField.getName)
             // logger.error(MESSAGE_ID_CNV1001, "invaild record found data : " + record)
              ""
          }
        }
      }

      val logger = new ECCLogger()
      logger.configure(pro)

      val configuration = new YarnConfiguration()
      for((key,value)<-hadoopConfigurationMap){
        configuration.set(key, value)
      }

      while(blockPositionIterator.hasNext){
        val (blockIndex, blockPosition, blockSize,filePath) = blockPositionIterator.next()
        val fileSystem = FileSystem.get(configuration)
        val iffFileInputPath = new Path(filePath)
        val iffFileInputStream = fileSystem.open(iffFileInputPath)
        val iffFileSourceInputStream =
          if(fileGzipFlgMap(filePath)) new GZIPInputStream(iffFileInputStream, readBufferSize)
          else iffFileInputStream
        val inputStream = iffFileSourceInputStream.asInstanceOf[java.io.InputStream]
        var restToSkip = blockPosition
        while (restToSkip > 0) {
          val skipped = inputStream.skip(restToSkip)
          restToSkip = restToSkip - skipped
        }

        dataFileProcessor.open(inputStream,blockSize)
        while (dataFileProcessor.haveMoreLine()) {
          val fields = dataFileProcessor.getLineFields()
          if (fields.size>0) {
            var dataInd = 0
            val dataMap = new util.HashMap[String,Any]()
            val sb = new StringBuffer()
            var success = true
            var errorMessage = ""
            var currentName = ""
            var currentValue = ""
            try {
              for (iffField <- tableInfo.body.fields if (!iffField.virtual)) {
                val fieldType = iffField.typeInfo
                currentName = iffField.name
                if(dataInd>=fields.length){
                  currentValue=""
                }else {
                  currentValue = fields(dataInd)
                }
                if(StringUtils.isNotBlank(currentValue)) {
                  fieldType match {
                    case fieldType: CInteger => dataMap.put(iffField.name ,currentValue.trim.toInt)
                    case fieldType: CDecimal => dataMap.put(iffField.name , currentValue.trim.toDouble)
                    case _ => dataMap.put(iffField.name , currentValue)
                  }
                }else{
                  dataMap.put(iffField.name , "")
                }
                dataInd += 1
              }
            } catch {
              case e: NumberFormatException =>
                success = false
                errorMessage = " " + currentName+"["+currentValue+"] String to Number Exception "
              case e: Exception =>
                success = false
                errorMessage = " " +currentName+"["+currentValue+"] unknown exception " + e.getMessage
            }
            if (success) {
              errorMessage = ""
              implicit val validContext = new CommonFieldValidatorContext
              import com.boc.iff.CommonFieldValidatorContext._
              for (iffField <- tableInfo.body.fields if success && !iffField.isFilter) {
                try {
                  success = if (iffField.validateField(dataMap)) true else false
                  if(success){
                    sb.append(convertField(iffField, dataMap)).append(fieldDelimiter) //`
                  }else{
                    errorMessage =  iffField.name + " ERROR validateField"
                  }
                }catch{
                  case e:Exception=>
                    success = false
                    errorMessage = iffField.name+" "+e.getMessage
                }
              }
            }
            if (!success) {
              sb.setLength(0)
              sb.append(fields.reduceLeft(_+tableInfo.srcSeparator+_)).append(lineSplit).append(errorMessage).append(" ERROR")
            }
            logger.info("EDFDdddd",sb.toString+"|")
            recordList += sb.toString
          }
        }
        dataFileProcessor.close()
      }
      recordList.iterator
    }
    convertByPartitionsFunction
  }

  private def getDataFileProcessor():DataFileProcessor={
    var dataFileProcessor:DataFileProcessor = null
    fileInfo.fileType match {
      case FileType.FIXLENGTH => dataFileProcessor = new FixDataFileProcessor
      case _ => dataFileProcessor = new UnfixDataFileProcessor
    }
    dataFileProcessor
  }

}
