package com.boc.iff.itf

import java.io.File
import com.boc.iff._
import com.boc.iff.IFFConversion._
import com.boc.iff.model._
import com.boc.iff.{DataProcessConfig, SparkJobConfig}
import org.apache.hadoop.fs.{FileSystem, Path}

class DataProcessOnSparkConfig extends DataProcessConfig with SparkJobConfig {

}

class DataProcessOnSparkJob
  extends DataProcess[DataProcessOnSparkConfig] with SparkJob[DataProcessOnSparkConfig] {

  protected def deleteTargetDir(path:String,dir:String) = {
    logger.info(MESSAGE_ID_CNV1001, "Delete Target Dir: " + path+"/"+dir)
    implicit val configuration = sparkContext.hadoopConfiguration
    DFSUtils.deleteDir(path+"/"+dir)
  }

  protected def getTempDir(dir:String): String = {
    dataProcessConfig.tempDir + "/" + dir
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
      !checkLocalFileExists(dataProcessConfig.metadataFilePath))
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
   * 准备阶段
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

  override protected def runOnSpark(jobConfig: DataProcessOnSparkConfig): Unit = {
    run(jobConfig)
  }

  /**
   * 执行整个作业
   */
  override protected def run(config: DataProcessOnSparkConfig): Unit = {
    this.dataProcessConfig = config
    if (!prepare()) return
    if(dbManagers.nonEmpty){
      for(dbManager<-dbManagers){
        dbManager.patchIFFConversionConfig(config)
      }
    }
    processFile()
    logger.info(MESSAGE_ID_CNV1001, "File Conversion Complete! File: " + config.datFileOutputPath)
  }

  override def processFile = {
  }
}


