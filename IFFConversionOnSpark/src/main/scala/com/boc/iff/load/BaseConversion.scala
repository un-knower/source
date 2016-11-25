package com.boc.iff.load

import java.io.{ File, FileInputStream}
import java.util
import java.util.Properties
import java.util.concurrent.LinkedBlockingQueue
import java.util.zip.GZIPInputStream

import com.boc.iff.DFSUtils.FileMode
import com.boc.iff._
import com.boc.iff.IFFConversion._
import com.boc.iff.exception._
import com.boc.iff.model._
import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}
import org.apache.hadoop.io.IOUtils
import org.apache.hadoop.yarn.conf.YarnConfiguration

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent._
import scala.concurrent.duration.Duration
import scala.util.{Failure, Success}

/**
  * @author www.birdiexx.com
  */
class BaseConversionOnSparkConfig extends IFFConversionConfig with SparkJobConfig {

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


/**
  * @author www.birdiexx.com
  */
trait BaseConversionOnSparkJob[T<:BaseConversionOnSparkConfig]
  extends IFFConversion[T] with SparkJob[T]  {
  val fileGzipFlgMap = new mutable.HashMap[String,Boolean]
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

  override protected def loadIFFFileInfo(iffFileName: String, readBufferSize: Int): Unit ={
    if(isDirectory(iffFileName)){
      iffFileInfo = new IFFFileInfo()
      iffFileInfo.gzip = false
      val sourceCharsetName =
        if(StringUtils.isNotEmpty(iffMetadata.sourceCharset)) {
          iffMetadata.sourceCharset
        }else{
          "UTF-8"
        }
      iffMetadata.sourceCharset = sourceCharsetName
      logger.info(MESSAGE_ID_CNV1001, "SrcCharset: " + sourceCharsetName)
      iffFileInfo.recordLength = iffMetadata.header.getLength
      iffFileInfo.recordLength = math.max(iffFileInfo.recordLength, iffMetadata.body.getLength)
      iffFileInfo.recordLength = math.max(iffFileInfo.recordLength, iffMetadata.footer.getLength)
      if(iffMetadata.fixedLength!=null) {
        iffFileInfo.recordLength = math.max(iffFileInfo.recordLength, iffMetadata.fixedLength.toInt)
      }
      logger.info(MESSAGE_ID_CNV1001, "iffFile REC_LENGTH = " + iffFileInfo.recordLength)
      iffFileInfo.dir = true
    }else{
      super.loadIFFFileInfo(iffFileName,readBufferSize)
    }

  }

  protected def isDirectory(fileName: String): Boolean = {
    logger.info("fileName",fileName)
    iffConversionConfig.iffFileMode match {
      case FileMode.LOCAL =>
        val f = new File(fileName)
        f.isDirectory()
      case FileMode.DFS =>
        val fileSystem = FileSystem.get(sparkContext.hadoopConfiguration)
        val filePath = new Path(fileName)
        fileSystem.isDirectory(filePath)
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
        if(iffFileInfo.isGzip) new GZIPInputStream(iffFileInputStream, iffConversionConfig.readBufferSize)
        else iffFileInputStream
    }
  }

  protected def getIFFFilePath(fileName: String):java.util.List[String] = {
    logger.info("fileName",fileName)
    val datafiles = new util.ArrayList[String]
    iffConversionConfig.iffFileMode match {
      case FileMode.LOCAL =>
        val f = new File(fileName)
        if(f.isDirectory){
          for(i<-f.listFiles() if !i.isDirectory){
            datafiles.add(i.getPath)
          }
        }else{
          datafiles.add(fileName)
        }
      case FileMode.DFS =>
        val fileSystem = FileSystem.get(sparkContext.hadoopConfiguration)
        val filePath = new Path(fileName)
        logger.info("fileSystem.isDirectory","fileSystem.isDirectory"+fileSystem.isDirectory(filePath))
        if(fileSystem.isDirectory(filePath)){
          logger.info("fileSystem.isDirectory","fileSystem.isDirectory")
          val fi = fileSystem.listFiles(filePath,false)
          while(fi.hasNext){
            val fs = fi.next()
            if(fs.isFile){
              logger.info("getPath****************","**********"+fs.getPath.toString)
              fileGzipFlgMap += (fs.getPath.toString->checkGZipFile(fs.getPath.toString))
              datafiles.add(fs.getPath.toString)
            }
          }
        }else{
          datafiles.add(fileName)
          fileGzipFlgMap += (fileName->checkGZipFile(fileName))
        }
    }
    datafiles
  }

  protected def createBlocks: java.util.concurrent.LinkedBlockingQueue[(Int, Long, Int,String)] = {
    val blockPositionQueue = new LinkedBlockingQueue[(Int, Long, Int,String)]()
    val filePaths = getIFFFilePath(iffConversionConfig.iffFileInputPath+iffConversionConfig.filename)
    val futureQueue = mutable.Queue[Future[java.util.concurrent.LinkedBlockingQueue[(Int, Long, Int)]]]()
    for(index<-(0 until filePaths.size).view){
      val filePath = filePaths.get(index)
      val conversionFuture = future { createBlockPositionQueue(filePath) }
      conversionFuture.onComplete{
        case Success(blockPq) =>
          for (j <- 0 until blockPq.size()) {
            val (blockIndex, blockPosition, blockSize) = blockPq.take()
            val block: (Int, Long, Int, String) = (blockIndex, blockPosition, blockSize, filePath)
            blockPositionQueue.put(block)
          }
        case Failure(ex) => throw ex
      }
      futureQueue.enqueue(conversionFuture)
    }
    while(futureQueue.nonEmpty){
      val future = futureQueue.dequeue()
      Await.ready(future, Duration.Inf)
    }
    logger.info("blockPositionQueue Info", "blockPositionQueueSize:"+blockPositionQueue.size())
    blockPositionQueue
  }

  protected def getDataFileProcessor():DataFileProcessor

  protected def createBlockPositionQueue(filePath:String): java.util.concurrent.LinkedBlockingQueue[(Int, Long, Int)]

  /**
    * 创建一个方法 对一个分片（分区）的数据进行转换操作
    *
    * @return
    */
  protected def createConvertOnDFSByPartitionsFunction: (Iterator[(Int, Long, Int,String)] => Iterator[String]) = {
    val iffConversionConfig = this.iffConversionConfig
    val lengthOfLineEnd = iffConversionConfig.lengthOfLineEnd
    val iffMetadata = this.iffMetadata
    val iffFileInfo = this.iffFileInfo
    val fieldDelimiter = this.fieldDelimiter
    val lineSplit = iffMetadata.srcSeparator
    val fileGzipFlgMap = this.fileGzipFlgMap
    val specialCharConvertor = this.specialCharConvertor
    val needConvertSpecialChar:Boolean = if("Y".equals(this.iffConversionConfig.specialCharConvertFlag))true else false
    implicit val configuration = sparkContext.hadoopConfiguration
    val hadoopConfigurationMap = mutable.HashMap[String,String]()
    val iterator = configuration.iterator()
    val prop = new Properties()
    prop.load(new FileInputStream(iffConversionConfig.configFilePath))
    while (iterator.hasNext){
      val entry = iterator.next()
      hadoopConfigurationMap += entry.getKey -> entry.getValue
    }
    val readBufferSize = iffConversionConfig.readBufferSize

    val dataFileProcessor = this.getDataFileProcessor()
    dataFileProcessor.iffMetadata = iffMetadata
    dataFileProcessor.lengthOfLineEnd = lengthOfLineEnd
    dataFileProcessor.needConvertSpecialChar = needConvertSpecialChar
    dataFileProcessor.specialCharConvertor = specialCharConvertor
    dataFileProcessor.iffFileInfo = iffFileInfo

    val convertByPartitionsFunction: (Iterator[(Int, Long, Int,String)] => Iterator[String]) = { blockPositionIterator =>
      val logger = new ECCLogger()
      logger.configure(prop)
      val recordList = ListBuffer[String]()

      import com.boc.iff.CommonFieldConvertorContext._
      val charset = IFFUtils.getCharset(iffMetadata.sourceCharset)
      dataFileProcessor.charset = charset
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
      while(blockPositionIterator.hasNext){
        val (blockIndex, blockPosition, blockSize,filePath) = blockPositionIterator.next()
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
            val dataMap = new mutable.HashMap[String, Any]
            val sb = new StringBuffer()
            var success = true
            var errorMessage = ""
            var currentName = ""
            var currentValue = ""
            try {
              for (iffField <- iffMetadata.body.fields if (!iffField.virtual)) {
                val fieldType = iffField.typeInfo
                currentName = iffField.name
                currentValue = fields(dataInd)
                //dataMap += (iffField.name+"DataStringValue" -> fields(dataInd))
                if(StringUtils.isNotBlank(fields(dataInd))) {
                  fieldType match {
                    case fieldType: CInteger => dataMap += (iffField.name -> fields(dataInd).trim.toInt)
                    case fieldType: CDecimal => dataMap += (iffField.name -> fields(dataInd).trim.toDouble)
                    case _ => dataMap += (iffField.name -> fields(dataInd))
                  }
                }else{
                  dataMap += (iffField.name -> "")
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
              for (iffField <- iffMetadata.body.fields if success && !iffField.isFilter) {
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
              sb.append(fields.reduceLeft(_+iffMetadata.srcSeparator+_)).append(lineSplit).append(errorMessage).append(" ERROR")
            }
            recordList += sb.toString
          }
        }
        dataFileProcessor.close()
      }
      recordList.iterator
    }
    convertByPartitionsFunction
  }

  protected def convertFileOnDFS(): Unit = {
    val iffConversionConfig = this.iffConversionConfig
    implicit val configuration = sparkContext.hadoopConfiguration
    val blockPositionQueue = createBlocks
    val convertByPartitions = createConvertOnDFSByPartitionsFunction
    val broadcast: org.apache.spark.broadcast.Broadcast[mutable.ListBuffer[Long]]
    = sparkContext.broadcast(mutable.ListBuffer())

    val tempDir = getTempDir
    val errorDir = getErrorFileDir
    DFSUtils.deleteDir(tempDir)
    DFSUtils.deleteDir(errorDir)
    val conversionJob: (Unit=>String) = { _=>
      val blockPosition = blockPositionQueue.take()
      val blockIndex = blockPosition._1
      val filePath = blockPosition._4
      val filename = filePath.substring(filePath.lastIndexOf("/")+1)
      logger.info("blockIndex",blockIndex.toString)
      logger.info("filePath",filePath)
      val rdd = sparkContext.makeRDD(Seq(blockPosition), 1)
      val convertedRecords = rdd.mapPartitions(convertByPartitions)
      val tempOutputDir = "%s/%s-%05d".format(tempDir, filename,blockIndex)
      val errorOutputDir = "%s/%s-%05d".format(errorDir, filename,blockIndex)
      logger.info(MESSAGE_ID_CNV1001, "[%s]Temporary Output: %s".format(Thread.currentThread().getName, tempOutputDir))
      val errorRcdRDD = convertedRecords.filter(_.endsWith("ERROR"))
      if(iffConversionConfig.fileMaxError>0) {
        val errorRecordNumber = errorRcdRDD.count()
        broadcast.value += errorRecordNumber
      }
      convertedRecords.filter(!_.endsWith("ERROR")).saveAsTextFile(tempOutputDir)
      errorRcdRDD.saveAsTextFile(errorOutputDir)
      convertedRecords.unpersist()

      /*
      val fileStatusArray = fileSystem.listStatus(new Path(tempOutputDir)).filter(_.getLen > 0)
      for (fileStatusIndex <- fileStatusArray.indices.view) {
        val fileStatus = fileStatusArray(fileStatusIndex)
        val fileName =
          if (fileStatusArray.length == 1) "%s/%s-%05d".format(iffConversionConfig.datFileOutputPath,filename, blockIndex)
          else "%s/%s-%05d-%05d".format(iffConversionConfig.datFileOutputPath,filename, blockIndex, fileStatusIndex)
        val srcPath = fileStatus.getPath
        val dstPath = new Path(fileName)
        DFSUtils.moveFile(srcPath, dstPath)
      }*/
      tempOutputDir
    }


    val futureQueue = mutable.Queue[Future[String]]()
    for(index<-(0 until blockPositionQueue.size()).view){
      val conversionFuture = future { conversionJob() }
      futureQueue.enqueue(conversionFuture)
    }
    while(futureQueue.nonEmpty){
      val future = futureQueue.dequeue()
      Await.ready(future, Duration.Inf)
    }
    combineMutilFileToSingleFile(errorDir)
    if(iffConversionConfig.fileMaxError>0) {
      val errorRec = broadcast.value.foldLeft(0L)(_ + _)
      logger.info("errorRec", "errorRec:" + errorRec)
      if (errorRec >= iffConversionConfig.fileMaxError) {
        throw MaxErrorNumberException("errorRec:" + errorRec + "iffConversionConfig.fileMaxError" + iffConversionConfig.fileMaxError)
      }
    }
    saveTargetData()
    DFSUtils.deleteDir(tempDir)
  }

  protected def combineMutilFileToSingleFile(path:String):Unit={
    val fileSystem = FileSystem.get(sparkContext.hadoopConfiguration)
    val sourceFilePath = new Path(path)
    val target = "%s/%s".format(path,"TARGET")
    val targetFilePath = new Path(target)
    val out = fileSystem.create(targetFilePath)
    val fileStatus = fileSystem.getFileStatus(sourceFilePath)
    val fileStatusStrack:mutable.Stack[FileStatus] = new mutable.Stack[FileStatus]()
    fileStatusStrack.push(fileStatus)
    while(!fileStatusStrack.isEmpty){
      val fst = fileStatusStrack.pop()
      if(fst.isDirectory){
        val fileStatusS = fileSystem.listStatus(fst.getPath)
        for(f<-fileStatusS){
          fileStatusStrack.push(f)
        }
      }else{
        val in = fileSystem.open(fst.getPath)
        IOUtils.copyBytes(in, out, 4096, false)
        in.close(); //完成后，关闭当前文件输入流
      }
    }
    out.close();
    val files = fileSystem.listStatus(sourceFilePath)

    for(f<-files){
      if(!f.getPath.toString.endsWith("TARGET")){
        fileSystem.delete(f.getPath, true)
      }
    }
  }

  protected def saveTargetData():Unit={
    if(iffConversionConfig.autoDeleteTargetDir)cleanDataFile()
    val fileSystem = FileSystem.get(sparkContext.hadoopConfiguration)
    val sourceFilePath = new Path(getTempDir)
    val fileStatus = fileSystem.getFileStatus(sourceFilePath)
    val fileStatusStrack:mutable.Stack[FileStatus] = new mutable.Stack[FileStatus]()
    fileStatusStrack.push(fileStatus)
    var index:Int = 0;
    while(!fileStatusStrack.isEmpty){
      val fst = fileStatusStrack.pop()
      if(fst.isDirectory){
        val fileStatusS = fileSystem.listStatus(fst.getPath)
        for(f<-fileStatusS){
          fileStatusStrack.push(f)
        }
      }else if(fst.getLen>0){
        val fileName =  "%s/%s-%03d".format(iffConversionConfig.datFileOutputPath,sparkContext.applicationId, index)
        val srcPath = fst.getPath
        val dstPath = new Path(fileName)
        DFSUtils.moveFile(fileSystem,srcPath, dstPath)
        index+=1
      }
    }
  }

  protected def cleanDataFile():Unit={
    logger.info("MESSAGE_ID_CNV1001","****************clean target dir ********************")
    deleteTargetDir()
    val fileSystem = FileSystem.get(sparkContext.hadoopConfiguration)
    val datFileOutputPath = new Path(iffConversionConfig.datFileOutputPath)
    if (!fileSystem.isDirectory(datFileOutputPath)){
      logger.info(MESSAGE_ID_CNV1001, "Create Dir: " + datFileOutputPath.toString)
      fileSystem.mkdirs(datFileOutputPath)
    }
  }

  /**
    * 转换 IFF 数据文件
    * @author www.birdiexx.com
    */
  override protected def convertFile(): Unit = {
    iffConversionConfig.iffFileMode match {
      case FileMode.DFS => convertFileOnDFS()
    }
  }

  /**
    * 准备阶段：
    * 1. 检查输入文件是否存在
    * 2. 加载配置文件
    * 3. 查询目标表信息
    * @author www.birdiexx.com
    * @return
    */
  override protected def prepare(): Boolean = {
    val result = super.prepare()
    if(iffConversionConfig.maxBlockSize > 0 && iffConversionConfig.minBlockSize > 0){
      if(!iffFileInfo.dir) {
        val fitBlockSize = (iffFileInfo.fileLength / (if (maxExecutors == 0) 1 else maxExecutors)) + (iffFileInfo.fileLength % (if (maxExecutors == 0) 1 else maxExecutors))
        iffConversionConfig.blockSize =
          math.min(iffConversionConfig.maxBlockSize, math.max(iffConversionConfig.minBlockSize, fitBlockSize)).toInt
      }else{
        iffConversionConfig.blockSize = iffConversionConfig.maxBlockSize
      }
    }
    numberOfThread = if(this.iffConversionConfig.iffNumberOfThread>0){
      this.iffConversionConfig.iffNumberOfThread
    }else if(dynamicAllocation) {
      maxExecutors
    }else{
      numExecutors
    }
    System.setProperty("scala.concurrent.context.maxThreads", String.valueOf(numberOfThread))
    logger.info(MESSAGE_ID_CNV1001, "Num Threads: " + numberOfThread)
    result
  }

  /**
    * 注册使用 kryo 进行序列化的类
    * @author www.birdiexx.com
    * @return
    **/
  override protected def kryoClasses: Array[Class[_]] = {
    Array[Class[_]](classOf[IFFMetadata], classOf[IFFSection], classOf[IFFField], classOf[IFFFileInfo],
      classOf[IFFFieldType], classOf[FormatSpec], classOf[ACFormat], classOf[StringAlign])
  }

  override protected def runOnSpark(jobConfig: T): Unit = {
    run(jobConfig)
  }

}

