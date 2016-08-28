package com.boc.iff


import com.boc.iff.DFSUtils.FileMode
import com.boc.iff.IFFConversion._
import com.boc.iff.model._
import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.fs.{FileSystem, Path}
import scala.collection.mutable

class New2FullOnSparkConfig extends IFFConversionConfig with SparkJobConfig {

  var iffFileMode: DFSUtils.FileMode.ValueType = DFSUtils.FileMode.LOCAL

  override protected def makeOptions(optionParser: scopt.OptionParser[_]) = {
    super.makeOptions(optionParser)
    optionParser.opt[String]("iff-file-mode")
    .text("IFF File Mode")
    .foreach{ x=> this.iffFileMode = DFSUtils.FileMode.withName(x) }
  }

  override def toString = {
    val builder = new mutable.StringBuilder(super.toString)
    if(builder.nonEmpty) builder ++= "\n"
    builder ++= "IFF File Mode: %s".format(iffFileMode.toString)
    builder.toString
  }
}

class New2FullOnSparkJob
  extends IFFConversion[New2FullOnSparkConfig] with SparkJob[New2FullOnSparkConfig] {

  protected def deleteTargetDir(): Unit = {
    logger.info(MESSAGE_ID_CNV1001, "Auto Delete Target Dir: " + iffConversionConfig.datFileOutputPath)
    implicit val configuration = sparkContext.hadoopConfiguration
    DFSUtils.deleteDir(iffConversionConfig.datFileOutputPath)
  }

  protected def getTempDir: String = {
    iffConversionConfig.tempDir + "/" + StringUtils.split(iffConversionConfig.iffFileInputPath, "/").last
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
    iffConversionConfig.iffFileMode match {
      case FileMode.LOCAL => super.openIFFFileInputStream(fileName)
      case FileMode.DFS =>
        val fileSystem = FileSystem.get(sparkContext.hadoopConfiguration)
        val filePath = new Path(fileName)
        val iffFileInputStream = fileSystem.open(filePath, iffConversionConfig.readBufferSize)
        iffFileInputStream
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
    numberOfThread =
      if (iffFileInfo.fileLength <= iffConversionConfig.blockSize) 1
      else (iffFileInfo.fileLength / iffConversionConfig.blockSize).toInt
    if(dynamicAllocation) {
      numberOfThread = math.max(math.min(numberOfThread, maxExecutors), 1)
    }else{
      numberOfThread = math.min(numberOfThread, numExecutors)
    }
    logger.info(MESSAGE_ID_CNV1001, "Num Threads: " + numberOfThread)
    result
  }

  /**
    * 注册使用 kryo 进行序列化的类
    *
    * @return
    **/
  override protected def kryoClasses: Array[Class[_]] = {
    Array[Class[_]](classOf[IFFMetadata], classOf[IFFSection], classOf[IFFField], classOf[IFFFileInfo])
  }

  override protected def runOnSpark(jobConfig: New2FullOnSparkConfig): Unit = {
    run(jobConfig)
  }

  /**
   * 执行整个作业
   */
  override protected def run(iffConversionConfig: New2FullOnSparkConfig): Unit = {
    this.iffConversionConfig = iffConversionConfig
    if(!prepare()) return
    convertFile()
    logger.info(MESSAGE_ID_CNV1001, "File Conversion Complete! File: " + iffConversionConfig.iffFileInputPath)
  }

  override def convertFile = {
    val aaa = sparkContext.textFile("file:///app/birdie/bochk/IFFConversion/config/config.properties")
    aaa.collect()
//    val newRDD = sc.parallelize(List(("A",1),("B",1)))
//    val fullRDD = sc.parallelize(List(("A",1),("B",1),("C",1),("D",1)))
//    val changeFullRDD = fullRDD.join(newRDD).filter(x=>if(x._2._1==1)true else false).map(x=>(x._1,1)).collect
//    fullRDD.filter(x=>if(changeFullRDD.contains(x)) false else true ).collect
  }
}

/**
  *  Spark 程序入口
  */
object New2FullOnSpark extends App{
  val config = new New2FullOnSparkConfig()
  val job = new New2FullOnSparkJob()
  val logger = job.logger
  try {
    job.start(config, args)
  } catch {
    case t: Throwable =>
      t.printStackTrace()
      if(StringUtils.isNotEmpty(t.getMessage)) logger.error(MESSAGE_ID_CNV1001, t.getMessage)
      System.exit(1)
  }
}
