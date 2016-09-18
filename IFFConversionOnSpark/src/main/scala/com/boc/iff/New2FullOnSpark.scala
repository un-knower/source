package com.boc.iff

import com.boc.iff.IFFConversion._
import com.boc.iff.model._
import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.fs.{FileSystem, Path}
import java.io.{BufferedInputStream, File, FileInputStream}

class New2FullOnSparkConfig extends DataProcessConfig with SparkJobConfig {

}

class New2FullOnSparkJob
  extends DataProcess[New2FullOnSparkConfig] with SparkJob[New2FullOnSparkConfig] {

  protected def deleteTargetDir(): Unit = {
    logger.info(MESSAGE_ID_CNV1001, "Auto Delete Target Dir: " + dataProcessConfig.dataFileOutputPath)
    implicit val configuration = sparkContext.hadoopConfiguration
    DFSUtils.deleteDir(dataProcessConfig.dataFileOutputPath)
  }

  protected def getTempDir: String = {
    dataProcessConfig.tempDir + "/" + StringUtils.split(dataProcessConfig.dataFileInputPath, "/").last
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
    if (!fileSystem.exists(path)) {
      logger.error(MESSAGE_ID_CNV1001, "File 	:" + path.toString + " not exists.")
      false
    } else true
  }
  /**
   * 检查本地文件路径是否存在
   *
   * @param fileName 文件路径
   * @return
   */
  protected def checkLocalFileExists(fileName: String): Boolean = {
    println("checkLocalFileExists File 	:" +fileName)
    val file = new File(fileName)
    if(!file.exists()) {
      logger.error(MESSAGE_ID_CNV1001, "File 	:" + file.getAbsolutePath + " not exists.")
      false
    }else true
  }

  /**
   * 检查文件路径是否存在
   *
   * @param fileName 文件路径
   * @return
   */
  protected def checkFileExists(fileName: String): Boolean = {
    println("checkFileExists File 	:" +fileName)
    checkDFSFileExists(fileName)
  }

  /**
   * 检查参数中定义的 配置文件、XML 元数据文件和 IFF 文件是否存在
   *
   * @return
   */
  override protected def checkFilesExists: Boolean = {
    if (!checkLocalFileExists(dataProcessConfig.configFilePath) ||
      !checkLocalFileExists(dataProcessConfig.metadataFilePath)
      || !checkFileExists(dataProcessConfig.dataFileInputPath))
    {
      false
    } else {
      true
    }
  }


  override protected def openFileInputStream(fileName: String): java.io.InputStream = {
    val fileSystem = FileSystem.get(sparkContext.hadoopConfiguration)
    val filePath = new Path(fileName)
    val fileInputStream = fileSystem.open(filePath, dataProcessConfig.readBufferSize)
    fileInputStream
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
  override protected def run(config: New2FullOnSparkConfig): Unit = {
    this.dataProcessConfig = config
    if (!prepare()) return
    processFile()
    logger.info(MESSAGE_ID_CNV1001, "File Conversion Complete! File: " + config.dataFileInputPath)
  }

  override def processFile = {
    val aaa = sparkContext.textFile("file:///app/birdie/bochk/IFFConversion/config/config.properties")
    aaa.collect()
    aaa.saveAsTextFile("/tmp/tzm1")
    val newRDD = sparkContext.parallelize(List(("A",99),("B",99)))
    val fullRDD = sparkContext.parallelize(List(("A",99),("B",99),("C",99),("D",99)))
    val fullRDDTable = sparkContext.parallelize(List(("A","1|2"),("B","1|66662"),("C","1|66667772"),("D","1|999992")))
    val fullRDD1 =  fullRDDTable.leftOuterJoin(newRDD)
    val bbb = fullRDD1.filter(x=>if(x._2._2.isEmpty) true else false).map(x=>x._2._1)
    bbb.collect
    bbb.saveAsTextFile("/tmp/tzm2")

    fullRDD1.foreach(x=>println(x._2._2.isEmpty))

  }
}

/**
 * Spark 程序入口
 */
object New2FullOnSpark extends App {
  val config = new New2FullOnSparkConfig()
  val job = new New2FullOnSparkJob()
  val logger = job.logger
  try {
    job.start(config, args)
  } catch {
    case t: Throwable =>
      t.printStackTrace()
      if (StringUtils.isNotEmpty(t.getMessage)) logger.error(MESSAGE_ID_CNV1001, t.getMessage)
      System.exit(1)
  }
}
