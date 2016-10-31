package com.boc.iff.load

import java.io.File
import java.util
import java.util.concurrent.LinkedBlockingQueue

import com.boc.iff.DFSUtils.FileMode
import com.boc.iff._
import com.boc.iff.IFFConversion._
import com.boc.iff.exception._
import com.boc.iff.model._
import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.fs.{FileSystem, Path}

import scala.collection.mutable
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent._
import scala.concurrent.duration.Duration
import scala.util.{Failure, Success}

/**
  * @author www.birdiexx.com
  */
class MutilConversionOnSparkConfig extends IFFConversionConfig with SparkJobConfig {

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
trait MutilConversionOnSparkJob[T<:MutilConversionOnSparkConfig]
  extends IFFConversion[T] with SparkJob[T]  {

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
          if(iffFileInfo.isHost) "boc61388"
          else "UTF-8"
        }
      iffMetadata.sourceCharset = sourceCharsetName
      logger.info(MESSAGE_ID_CNV1001, "SrcCharset: " + sourceCharsetName)
      val charset = IFFUtils.getCharset(sourceCharsetName)
      val decoder = charset.newDecoder
      // record start position is not include record indicator. so 1 + header.getLength()
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
        iffFileInputStream
    }
  }

  protected def getIFFFilePath(fileName: String):java.util.List[String] = {
    logger.info("fileName",fileName)
    val datafiles = new util.ArrayList[String];
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
              datafiles.add(fs.getPath.toString)
            }
          }
        }else{
          datafiles.add(fileName)
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

  protected def createBlockPositionQueue(filePath:String): java.util.concurrent.LinkedBlockingQueue[(Int, Long, Int)]

  /**
    * 创建一个方法 对一个分片（分区）的数据进行转换操作
    * @author www.birdiexx.com
    * @return
    */
  protected def createConvertOnDFSByPartitionsFunction: (Iterator[(Int, Long, Int,String)] => Iterator[String])

  protected def convertFileOnDFS(): Unit = {
    val iffConversionConfig = this.iffConversionConfig
    implicit val configuration = sparkContext.hadoopConfiguration
    val fileSystem = FileSystem.get(configuration)
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
      //logger.info("tempOutputDir",tempOutputDir+"error/"+errorDir+convertedRecords.filter(_.endsWith("ERROR validateField")).count())

      val fileStatusArray = fileSystem.listStatus(new Path(tempOutputDir)).filter(_.getLen > 0)
      for (fileStatusIndex <- fileStatusArray.indices.view) {
        val fileStatus = fileStatusArray(fileStatusIndex)
        val fileName =
          if (fileStatusArray.length == 1) "%s/%s-%05d".format(iffConversionConfig.datFileOutputPath,filename, blockIndex)
          else "%s/%s-%05d-%05d".format(iffConversionConfig.datFileOutputPath,filename, blockIndex, fileStatusIndex)
        val srcPath = fileStatus.getPath
        val dstPath = new Path(fileName)
        DFSUtils.moveFile(srcPath, dstPath)
      }
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
    if(iffConversionConfig.fileMaxError>0) {
      val errorRec = broadcast.value.foldLeft(0L)(_ + _)
      logger.info("errorRec", "errorRec:" + errorRec)
      if (errorRec > iffConversionConfig.fileMaxError) {
        throw MaxErrorNumberException("errorRec:" + errorRec + "iffConversionConfig.fileMaxError" + iffConversionConfig.fileMaxError)
      }
    }
    DFSUtils.deleteDir(tempDir)
  }

  /**
    * 转换 IFF 数据文件
    * @author www.birdiexx.com
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
    numberOfThread =
    if(dynamicAllocation) {
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
