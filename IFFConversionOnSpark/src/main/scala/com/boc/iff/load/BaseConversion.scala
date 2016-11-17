package com.boc.iff.load

import java.text.SimpleDateFormat
import java.util.Date
import java.util.zip.GZIPInputStream

import com.boc.iff.DFSUtils.FileMode
import com.boc.iff._
import com.boc.iff.IFFConversion._
import com.boc.iff.exception._
import com.boc.iff.model._
import com.boc.iff.{DFSUtils, IFFConversionConfig, IFFUtils, SparkJobConfig}
import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.fs._
import org.apache.hadoop.io.IOUtils

import scala.collection.mutable
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent._
import scala.concurrent.duration.Duration

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

trait BaseConversionOnSparkJob[T<:BaseConversionOnSparkConfig]
  extends IFFConversion[T] with SparkJob[T] {

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
    *@author www.birdiexx.com
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
    *@author www.birdiexx.com
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
    *@author www.birdiexx.com
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
        if(iffFileInfo.isGzip) new GZIPInputStream(iffFileInputStream, iffConversionConfig.readBufferSize)
        else iffFileInputStream
    }
  }

  protected def createBlockPositionQueue: java.util.concurrent.LinkedBlockingQueue[(Int, Long, Int)]

  /**
    * 创建一个方法 对一个分片（分区）的数据进行转换操作
    * @author www.birdiexx.com
    * @return
    */
  protected def createConvertOnDFSByPartitionsFunction: (Iterator[(Int, Long, Int)] => Iterator[String])

  protected def convertFileOnDFS(): Unit = {
    val iffConversionConfig = this.iffConversionConfig
    implicit val configuration = sparkContext.hadoopConfiguration
    val fileSystem = FileSystem.get(configuration)
    val blockPositionQueue = createBlockPositionQueue

    val convertByPartitions = createConvertOnDFSByPartitionsFunction
    val broadcast: org.apache.spark.broadcast.Broadcast[mutable.ListBuffer[Long]]
    = sparkContext.broadcast(mutable.ListBuffer())
    sparkContext.applicationId
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
      logger.info("blockIndex",blockIndex.toString)
      val tempOutputDir = "%s/%05d".format(tempDir, blockIndex)
      val errorOutputDir = "%s/%05d".format(errorDir, blockIndex)
      logger.info(MESSAGE_ID_CNV1001, "[%s]Temporary Output: %s".format(Thread.currentThread().getName, tempOutputDir))
      logger.info("RDD Cache begin","************"+blockIndex+" "+new java.util.Date())
      convertedRecords.cache()
      logger.info("RDD Cache end","************"+blockIndex+" "+new java.util.Date())
      logger.info("RDD filter error begin","************"+blockIndex+" "+new java.util.Date())
      val errorRcdRDD = convertedRecords.filter(_.endsWith("ERROR"))
      logger.info("RDD filter error end","************"+blockIndex+" "+new java.util.Date())

      if(iffConversionConfig.fileMaxError>0) {
        val errorRecordNumber = errorRcdRDD.count()
        broadcast.value += errorRecordNumber
      }
      logger.info("RDD FILTER SAVE END","************"+blockIndex+" "+new java.util.Date())
      convertedRecords.filter(!_.endsWith("ERROR")).saveAsTextFile(tempOutputDir)
      logger.info("RDD FILTER SAVE END","************"+blockIndex+" "+new java.util.Date())
      errorRcdRDD.saveAsTextFile(errorOutputDir)
      convertedRecords.unpersist()
      /*
      val fileStatusArray = fileSystem.listStatus(new Path(tempOutputDir)).filter(_.getLen > 0)
      val dateStr = sparkContext.applicationId
      for (fileStatusIndex <- fileStatusArray.indices.view) {
        val fileStatus = fileStatusArray(fileStatusIndex)
        val fileName =
          if (fileStatusArray.length == 1) "%s/%s-%05d".format(iffConversionConfig.datFileOutputPath,dateStr, blockIndex)
          else "%s/%s-%05d-%05d".format(iffConversionConfig.datFileOutputPath,dateStr, blockIndex, fileStatusIndex)
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
        throw MaxErrorNumberException("errorRecException:File error recordNumber is bigger than limited, File error recordNumber:" + errorRec + ", file limited errorNumber" + iffConversionConfig.fileMaxError)
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
    implicit val configuration = sparkContext.hadoopConfiguration
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
      }else{
        val fileName =  "%s/%s-%03d".format(iffConversionConfig.datFileOutputPath,sparkContext.applicationId, index)
        val srcPath = fileStatus.getPath
        val dstPath = new Path(fileName)
        DFSUtils.moveFile(srcPath, dstPath)
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
    //if(iffConversionConfig.autoDeleteTargetDir)cleanDataFile()
    iffConversionConfig.iffFileMode match {
      case FileMode.DFS => convertFileOnDFS()
    }
  }

  /**
    * 准备阶段：
    * 1. 检查输入文件是否存在
    * 2. 加载配置文件
    * 3. 查询目标表信息
    *@author www.birdiexx.com
    * @return
    */
  override protected def prepare(): Boolean = {
    val result = super.prepare()
    if(iffConversionConfig.maxBlockSize > 0 && iffConversionConfig.minBlockSize > 0){
      val fitBlockSize = (iffFileInfo.fileLength / (if(maxExecutors==0)1 else maxExecutors)) + (iffFileInfo.fileLength % (if(maxExecutors==0)1 else maxExecutors))
      iffConversionConfig.blockSize =
        math.min(iffConversionConfig.maxBlockSize, math.max(iffConversionConfig.minBlockSize, fitBlockSize)).toInt
    }
    numberOfThread = if(this.iffConversionConfig.iffNumberOfThread>0){
      this.iffConversionConfig.iffNumberOfThread
    }else if(dynamicAllocation) {
      maxExecutors
    }else{
      numExecutors
    }
    System.setProperty("scala.concurrent.context.numThreads", String.valueOf(numberOfThread))
    System.setProperty("scala.concurrent.context.maxThreads", String.valueOf(numberOfThread))
    logger.info(MESSAGE_ID_CNV1001, "Num Threads: " + numberOfThread)
    result
  }

  /**
    * 注册使用 kryo 进行序列化的类
    *@author www.birdiexx.com
    *
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
