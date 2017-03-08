package com.boc.iff.history

import java.io.File

import com.boc.iff.IFFConversion._
import com.boc.iff.itf.DataProcessOnSparkConfig
import com.boc.iff.model._
import com.boc.iff.{DataProcessConfig, SparkJobConfig, _}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{DataFrame, SQLContext}

class HistoryProcessOnSparkConfig extends DataProcessConfig with SparkJobConfig {

}

abstract class HistoryProcessOnSparkJob
  extends HistoryProcess[DataProcessOnSparkConfig,DataFrame] with SparkJob[DataProcessOnSparkConfig] with DataReader with DataWriter {

  protected var sqlContext:SQLContext = null

  protected val beginDTName:String = "begin_date"
  protected val endDTName:String = "end_date"

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
   * 检查参数中定义的 配置文件、XML 元数据文件是否存在
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

  /**
   * 准备阶段
   *
   * @return
   */
  override protected def prepare(): Boolean = {
       var result = super.prepare()
       if(result){
         try{
           sqlContext = new SQLContext(this.sparkContext)
         }catch {
           case e:Exception => result = false
         }
       }
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
    val incDF = getIncrease(dataProcessConfig.iTableDatFilePath,this.iffMetadata)
    if(incDF!=null) {
      val hisDF = getHistory(dataProcessConfig.fTableDatFilePath, this.iffMetadata)
      val (closeDF, openDF) = diffHistoryAndIncrease(incDF, hisDF, this.iffMetadata)
      appendHistory(closeDF, openDF)
    }

  }

  override def getIncrease(fileName: String,iffMetadata: IFFMetadata):DataFrame ={
    getInc(fileName,iffMetadata,sqlContext,fieldDelimiter)
  }

  override def getHistory(fileName: String,iffMetadata: IFFMetadata):DataFrame={
    getHis(fileName,iffMetadata,sqlContext,fieldDelimiter)
  }


  /**
   * 写拉链历史数据到目标目录
   */
  override def appendHistory(closeDF:DataFrame,openDF:DataFrame): Unit={
    val tmp = getTempDir(dataProcessConfig.fTableName)
    logger.info(MESSAGE_ID_CNV1001,"clean tmp table")
    implicit val configuration = sparkContext.hadoopConfiguration
    DFSUtils.deleteDir(tmp)
    if(closeDF!=null) {
      val closeTmpPath = "%s/%s".format(tmp, "close")
      writeData(closeDF, closeTmpPath, fieldDelimiter)
    }
    if(openDF!=null) {
      val openTmpPath = "%s/%s".format(tmp, "open")
      writeData(openDF, openTmpPath, fieldDelimiter)
    }
    saveToTarget(tmp,dataProcessConfig.fTableDatFilePath,sparkContext)
  }
}


