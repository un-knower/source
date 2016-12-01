package com.boc.iff.itf

import com.boc.iff.DFSUtils
import com.boc.iff.IFFConversion._
import com.boc.iff.model._
import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.fs.{FileSystem, Path}

import scala.collection.mutable.ArrayBuffer

class I2FWithCpOnSparkJob extends DataProcessOnSparkJob with Serializable {

  override def processFile = {
    println(this.dataProcessConfig.toString);

    val newRDD = sparkContext.textFile(this.dataProcessConfig.iTableDatFilePath)
    val tempDir = getTempDir(dataProcessConfig.fTableName)
    implicit val configuration = sparkContext.hadoopConfiguration
    //删除临时目录
    DFSUtils.deleteDir(tempDir)
    newRDD.saveAsTextFile(tempDir)

    if(dataProcessConfig.autoDeleteTargetDir) {
      logger.info("MESSAGE_ID_CNV1001", "****************clean target dir ********************")
      logger.info(MESSAGE_ID_CNV1001, "Auto Delete Target Dir: " + dataProcessConfig.fTableDatFilePath)
      implicit val configuration = sparkContext.hadoopConfiguration
      DFSUtils.deleteDir(dataProcessConfig.fTableDatFilePath)
      DFSUtils.createDir(dataProcessConfig.fTableDatFilePath)
    }
    //将没有改变的数据转移到目标表目录
    val fileSystem = FileSystem.get(configuration)
    val fileStatusArray = fileSystem.listStatus(new Path(tempDir)).filter(_.getLen > 0)
    var fileIndex = 0
    for (fileStatus <- fileStatusArray) {
      val fileName = "%s/%s-%05d".format(dataProcessConfig.fTableDatFilePath,sparkContext.applicationId, fileIndex)
      val srcPath = fileStatus.getPath
      val dstPath = new Path(fileName)
      DFSUtils.moveFile(srcPath, dstPath)
      fileIndex += 1
    }
  }
}

/**
 * Spark 程序入口
 */
object I2FWithCpOnSpark extends App {
  val config = new DataProcessOnSparkConfig()
  val job = new I2FWithCpOnSparkJob()
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
