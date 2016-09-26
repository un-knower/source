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

class UnfixedConversionOnSparkConfig extends IFFConversionConfig with SparkJobConfig {

  var iffFileMode: DFSUtils.FileMode.ValueType = DFSUtils.FileMode.LOCAL
  var maxBlockSize: Int = -1                                                //最大每次读取文件的块大小
  var minBlockSize: Int = -1                                                //最小每次读取文件的块大小

  override protected def makeOptions(optionParser: scopt.OptionParser[_]) = {
    super.makeOptions(optionParser)
    optionParser.opt[String]("iff-file-mode")
    .text("IFF File Mode")
    .foreach{ x=> this.iffFileMode = DFSUtils.FileMode.withName(x) }
    optionParser.opt[String]("max-block-size")
      .text("Max Block Size")
      .foreach { x=>this.maxBlockSize = IFFUtils.getSize(x) }
    optionParser.opt[String]("min-block-size")
      .text("Min Block Size")
      .foreach { x=>this.minBlockSize = IFFUtils.getSize(x) }
  }

  override def toString = {
    val builder = new mutable.StringBuilder(super.toString)
    if(builder.nonEmpty) builder ++= "\n"
    builder ++= "IFF File Mode: %s\n".format(iffFileMode.toString)
    builder ++= "Max Block Size: %d\n".format(maxBlockSize)
    builder ++= "Min Block Size: %d".format(minBlockSize)
    builder.toString
  }
}

class UnfixedConversionOnSparkJob
  extends IFFConversion[UnfixedConversionOnSparkConfig] with SparkJob[UnfixedConversionOnSparkConfig] {

  protected def deleteTargetDir(): Unit = {
    logger.info(MESSAGE_ID_CNV1001, "Auto Delete Target Dir: " + iffConversionConfig.datFileOutputPath)
    implicit val configuration = sparkContext.hadoopConfiguration
    DFSUtils.deleteDir(iffConversionConfig.datFileOutputPath)
  }

  protected def getTempDir: String = {
    iffConversionConfig.tempDir + "/" + StringUtils.split(iffConversionConfig.iffFileInputPath, "/").last
  }

  protected def getErrorFileDir: String = {
    val filePath = iffConversionConfig.errorFilDir + "/" + iffConversionConfig.iTableName
    implicit val configuration = sparkContext.hadoopConfiguration
    DFSUtils.createDir(filePath)
    filePath
  }

  /**
    * 检查DFS上文件路径是否存在
    *
    * @param fileName 文件路径
    * @return
    */
  protected def checkDFSFileExists(fileName: String): Boolean = {
    val fileSystem = FileSystem.get(sparkContext.hadoopConfiguration)
    val path = new Path(fileName)
    if(!fileSystem.exists(path)) {
      logger.error(MESSAGE_ID_CNV1001, "File 	:" + path.toString + " not exists.")
      false
    }else true
  }

  /**
    * 检查文件路径是否存在
    *
    * @param fileName 文件路径
    * @return
    */
  protected def checkFileExists(fileName: String,
                                fileMode: FileMode.ValueType = FileMode.LOCAL): Boolean = {
    fileMode match {
      case FileMode.LOCAL => checkLocalFileExists(fileName)
      case FileMode.DFS => checkDFSFileExists(fileName)
    }
  }

  /**
    * 检查参数中定义的 配置文件、XML 元数据文件和 IFF 文件是否存在
    *
    * @return
    */
  override protected def checkFilesExists: Boolean = {
    if(!checkFileExists(iffConversionConfig.configFilePath)||
      !checkFileExists(iffConversionConfig.metadataFilePath)||
      !checkFileExists(iffConversionConfig.iffFileInputPath, iffConversionConfig.iffFileMode)) {
      false
    } else {
      true
    }
  }


  override protected def getIFFFileLength(fileName: String, isGZip: Boolean): Long = {
    iffConversionConfig.iffFileMode match {
      case FileMode.LOCAL => super.getIFFFileLength(fileName, isGZip)
      case FileMode.DFS =>
        val fileSystem = FileSystem.get(sparkContext.hadoopConfiguration)
        val filePath = new Path(fileName)
        val fileStatus = fileSystem.getFileStatus(filePath)
        if(isGZip) fileStatus.getLen * 10
        else fileStatus.getLen
    }
  }

  override protected def openIFFFileInputStream(fileName: String): java.io.InputStream = {
    logger.info("fileName",fileName)
    iffConversionConfig.iffFileMode match {
      case FileMode.LOCAL => super.openIFFFileInputStream(fileName)
      case FileMode.DFS =>
        val fileSystem = FileSystem.get(sparkContext.hadoopConfiguration)
        val filePath = new Path(fileName)
        val iffFileInputStream = fileSystem.open(filePath, iffConversionConfig.readBufferSize)
        iffFileInputStream
    }
  }

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
    val needCheckBlank: Boolean = if(iffConversionConfig.fileMaxBlank==0) true else false

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
                throw RecordNumberErrorException("file "+iffConversionConfig.filename+ " number is not right "+recNum.toInt+countLineNumber)
              }
            }
          }else {//. 检查记录是否空行
            if (StringUtils.isEmpty(lineStr.trim)) {
              blankLineNumber += 1
              if (needCheckBlank && blankLineNumber > iffConversionConfig.fileMaxBlank) {
                logger.error("file " + iffConversionConfig.filename + " blank number error :" + blankLineNumber + " iffMetadata.body" + iffMetadata.body.getSourceLength, "blank number error")
                throw MaxBlankNumberException("file " + iffConversionConfig.filename + " blank number error:" + blankLineNumber + " iffMetadata.body" + iffMetadata.body.getSourceLength)
              }
            } else {
              //. 检查记录的列数
              if (countLineNumber == 0 ) {
                val lineSeq = StringUtils.splitByWholeSeparatorPreserveAllTokens(lineStr,iffMetadata.srcSeparator)
                if((lineSeq.length -1)!= iffMetadata.body.getSourceLength) {
                  logger.error("file " + iffConversionConfig.filename + " record column error lineStr:" + lineSeq.length + " iffMetadata.body" + iffMetadata.body.getSourceLength, "file number is not right")
                  throw RecordNotFixedException("file " + iffConversionConfig.filename + " record column error lineStr:" + lineSeq.length + " iffMetadata.body" + iffMetadata.body.getSourceLength)
                }
              }
              //换行符号
              currentLineLength = lineStr.getBytes(this.iffMetadata.sourceCharset).length+1
              logger.info("block Info", "第" + blockIndex + "块" + currentLineLength+" data:="+lineStr)
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
        if (iffField.isFiller) ""
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
        logger.info("blockInfo","posistionQueus("+blockIndex+","+blockPosition+","+blockSize+")")
        logger.info("blockPositionIterator.hasNext:","blockPositionIterator.hasNext")
        var currentBlockReadBytesCount: Long = 0
        var restToSkip = blockPosition
        while (restToSkip > 0) {
          val skipped = inputStream.skip(restToSkip)
          restToSkip = restToSkip - skipped
        }
        val br = new BufferedReader(new InputStreamReader(inputStream,charset))
        logger.info("chartSet","chartSet"+iffMetadata.sourceCharset)
        while ( currentBlockReadBytesCount < blockSize ) {
          val currentLine = br.readLine()
          logger.info("currentLine:","currentLine"+currentLine)
         var recordLength: Int = 0
          try {
            recordLength = currentLine.getBytes(iffMetadata.sourceCharset).length
          }catch {
            case e:Exception =>
              logger.info("currentLine:","currentLine"+currentLine+" currentBlockReadBytesCount:"+currentBlockReadBytesCount)
              throw e
          }
          currentBlockReadBytesCount += recordLength
          currentBlockReadBytesCount += 1
          val lineSeq = StringUtils.splitByWholeSeparatorPreserveAllTokens(currentLine,lineSplit)

          var dataInd = 0
          val dataMap = new mutable.HashMap[String,Any]
          val sb = new mutable.StringBuilder(recordLength)
          var success = true
          var errorMessage = "";
          try{
            for (iffField <- iffMetadata.body.fields if(StringUtils.isEmpty(iffField.getExpression))) {
              val fieldType = iffField.typeInfo
              fieldType match {
                case fieldType: CInteger => dataMap += (iffField.name -> lineSeq(dataInd).toInt)
                case fieldType: CDecimal => dataMap += (iffField.name -> lineSeq(dataInd).toDouble)
                case _ => dataMap += (iffField.name -> lineSeq(dataInd))
              }
              dataInd += 1
            }
          }catch{
            case e:NumberFormatException =>
              success = false
              errorMessage = " String to Number Exception "
            case e:Exception =>
              success = false
              errorMessage = " unknown exception "+e.getMessage
          }
          if(success){
            import com.boc.iff.CommonFieldValidatorContext._
            implicit val validContext = new CommonFieldValidatorContext
            for (iffField <- iffMetadata.body.fields if success && !iffField.isFiller) {
              sb ++= convertField(iffField, dataMap) //调用上面定义的闭包方法转换一个字段的数据
              sb ++= fieldDelimiter
              success = if(iffField.validateField(dataMap))true else false
              errorMessage = if(!success)"ERROR validateField" else ""
            }
          }
          if(!success){
            sb.setLength(0)
            sb.append(currentLine).append(lineSplit).append(errorMessage)
            logger.error("ERROR validateField",currentLine)
          }
          recordList += sb.toString
        }
      }
      iffFileSourceInputStream.close()
      recordList.iterator
    }
    convertByPartitionsFunction
  }

  protected def convertFileOnDFS(): Unit = {
    val iffConversionConfig = this.iffConversionConfig
    implicit val configuration = sparkContext.hadoopConfiguration
    val fileSystem = FileSystem.get(configuration)
    val blockPositionQueue = createBlockPositionQueue
    val convertByPartitions = createConvertOnDFSByPartitionsFunction
    val countList = new mutable.HashMap[String,Long] //with mutable.SynchronizedMap[String,Long]


    val tempDir = getTempDir
    val errorDir = getErrorFileDir
    DFSUtils.deleteDir(tempDir)
    DFSUtils.deleteDir(errorDir)
    val conversionJob: (Unit=>String) = { _=>
      val blockPosition = blockPositionQueue.take()
      val blockIndex = blockPosition._1
      logger.info("blockIndex",blockIndex.toString)
      val rdd = sparkContext.makeRDD(Seq(blockPosition), 1)
      val convertedRecords = rdd.mapPartitions(convertByPartitions)
      val tempOutputDir = "%s/%05d".format(tempDir, blockIndex)
      val errorOutputDir = "%s/%05d".format(errorDir, blockIndex)
      logger.info(MESSAGE_ID_CNV1001, "[%s]Temporary Output: %s".format(Thread.currentThread().getName, tempOutputDir))
      val errorRecordNumber = convertedRecords.filter(_.endsWith("ERROR validateField")).count()
      countList.synchronized{
        countList += blockIndex.toString->errorRecordNumber
      }
      convertedRecords.filter(!_.endsWith("ERROR validateField")).saveAsTextFile(tempOutputDir)
      if(convertedRecords.filter(_.endsWith("ERROR validateField")).count()>0){
        convertedRecords.filter(_.endsWith("ERROR validateField")).saveAsTextFile(errorOutputDir)
      }
      logger.info("tempOutputDir",tempOutputDir+"error/"+errorDir+convertedRecords.filter(_.endsWith("ERROR validateField")).count())

      val fileStatusArray = fileSystem.listStatus(new Path(tempOutputDir)).filter(_.getLen > 0)
      for (fileStatusIndex <- fileStatusArray.indices.view) {
        val fileStatus = fileStatusArray(fileStatusIndex)
        val fileName =
          if (fileStatusArray.length == 1) "%s/%05d".format(iffConversionConfig.datFileOutputPath, blockIndex)
          else "%s/%05d-%05d".format(iffConversionConfig.datFileOutputPath, blockIndex, fileStatusIndex)
        val srcPath = fileStatus.getPath
        val dstPath = new Path(fileName)
        DFSUtils.moveFile(srcPath, dstPath)
      }
      tempOutputDir
    }

    for(index<-(0 until blockPositionQueue.size()).view){
      logger.info("conversionJobCall","conversionJobCall")
      conversionJob()
    }

    /*val futureQueue = mutable.Queue[Future[String]]()
    for(index<-(0 until blockPositionQueue.size()).view){
      val conversionFuture = future { conversionJob() }
      futureQueue.enqueue(conversionFuture)
    }
    while(futureQueue.nonEmpty){
      val future = futureQueue.dequeue()
      Await.ready(future, Duration.Inf)
    }*/
    val errorRec = countList.values.foldLeft(0L)(_+_)
    if(errorRec>iffConversionConfig.fileMaxError){
      throw MaxErrorNumberException("errorRec:"+errorRec+"iffConversionConfig.fileMaxError"+iffConversionConfig.fileMaxError)
    }
    DFSUtils.deleteDir(tempDir)
  }

  /**
    * 转换 IFF 数据文件
    */
  override protected def convertFile(): Unit = {
    if(iffConversionConfig.autoDeleteTargetDir) deleteTargetDir()
    val fileSystem = FileSystem.get(sparkContext.hadoopConfiguration)
    val datFileOutputPath = new Path(iffConversionConfig.datFileOutputPath)
    if (!fileSystem.isDirectory(datFileOutputPath)){
      logger.info(MESSAGE_ID_CNV1001, "Create Dir: " + datFileOutputPath.toString)
      fileSystem.mkdirs(datFileOutputPath)
    }
    iffConversionConfig.iffFileMode match {
      case FileMode.DFS => convertFileOnDFS()
    }
  }

  /**
    * 准备阶段：
    * 1. 检查输入文件是否存在
    * 2. 加载配置文件
    * 3. 查询目标表信息
    *
    * @return
    */
  override protected def prepare(): Boolean = {
    val result = super.prepare()
    if(iffConversionConfig.maxBlockSize > 0 && iffConversionConfig.minBlockSize > 0){
      val fitBlockSize = (iffFileInfo.fileLength / (if(maxExecutors==0)1 else maxExecutors)) + (iffFileInfo.fileLength % (if(maxExecutors==0)1 else maxExecutors))
      iffConversionConfig.blockSize =
        math.min(iffConversionConfig.maxBlockSize, math.max(iffConversionConfig.minBlockSize, fitBlockSize)).toInt
    }
    numberOfThread =
      if (iffFileInfo.fileLength <= iffConversionConfig.blockSize) 1
      else {
        val fix = math.min(iffFileInfo.fileLength % iffConversionConfig.blockSize, 1).toInt
        (iffFileInfo.fileLength / iffConversionConfig.blockSize).toInt + fix
      }
    if(dynamicAllocation) {
      numberOfThread = math.max(math.min(numberOfThread, maxExecutors), 1)
    }else{
      numberOfThread = math.min(numberOfThread, numExecutors)
    }
    System.setProperty("scala.concurrent.context.maxThreads", String.valueOf(numberOfThread))
    logger.info(MESSAGE_ID_CNV1001, "Num Threads: " + numberOfThread)
    result
  }

  /**
    * 注册使用 kryo 进行序列化的类
    *d
    *
    * @return
    **/
  override protected def kryoClasses: Array[Class[_]] = {
    Array[Class[_]](classOf[IFFMetadata], classOf[IFFSection], classOf[IFFField], classOf[IFFFileInfo],
      classOf[IFFFieldType], classOf[FormatSpec], classOf[ACFormat], classOf[StringAlign])
  }

  override protected def runOnSpark(jobConfig: UnfixedConversionOnSparkConfig): Unit = {
    run(jobConfig)
  }
}
/**
  *  Spark 程序入口
  */
object UnfixedConversionOnSpark extends App{
  val config = new UnfixedConversionOnSparkConfig()
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
      System.exit(1)
    case t:RecordNotFixedException =>
      t.printStackTrace()
      System.exit(1)
    case t:MaxBlankNumberException =>
      t.printStackTrace()
      System.exit(1)
    case t: Throwable =>
      t.printStackTrace()
      if(StringUtils.isNotEmpty(t.getMessage)) logger.error(MESSAGE_ID_CNV1001, t.getMessage)
      System.exit(1)
  }
}
