package com.boc.iff.itf

import java.util

import com.boc.iff.DFSUtils
import com.boc.iff.IFFConversion._
import com.boc.iff.model._
import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql._
import org.apache.spark.sql.types.{DataTypes, StructField}

import scala.collection.mutable.ArrayBuffer

class I2FWithDFOnSparkJob  extends DataProcessOnSparkJob with Serializable {

  override def processFile = {
    println(this.dataProcessConfig.toString);

    //删除dataProcessConfig.tempDir
    val fields: List[IFFField] = iffMetadata.getBody.fields
    val tableFields = fields.filter(x=>(!"Y".equals(x.filter)))  //
    val primaryFields :List[IFFField]= fields.filter(x=>"Y".equals(x.primaryKey)) //
    var pkPosition = ArrayBuffer[Int]()
    for(v<-primaryFields){
      pkPosition +=tableFields.indexOf(v)
    }

    val basePk2Map= (x:String) => {
      val rowData = x.split(this.fieldDelimiter)
      var key=""
      for(v<-pkPosition){
        key+=rowData(v).trim
      }
      Row(key,x)
    }
    val structFields = new util.ArrayList[StructField]()
    structFields.add(DataTypes.createStructField("id",DataTypes.StringType,true))
    structFields.add(DataTypes.createStructField("content",DataTypes.StringType,true))
    val structType = DataTypes.createStructType(structFields)
    val sqlContext = new SQLContext(sparkContext)
    logger.info(MESSAGE_ID_CNV1001,"create DF begin "+new java.util.Date())
    val newDF = sqlContext.createDataFrame(sparkContext.textFile(this.dataProcessConfig.iffFileInputPath).map(basePk2Map),structType)
    val fullDF = sqlContext.createDataFrame(sparkContext.textFile(this.dataProcessConfig.datFileOutputPath).map(basePk2Map),structType)
    logger.info(MESSAGE_ID_CNV1001,"create DF end "+new java.util.Date())
    fullDF.registerTempTable("fullTB")
    newDF.registerTempTable("newTB")
    val sql = "select f.* from fullTB f left join newTB n on f.id = n.id where n.id is null "
    logger.info(MESSAGE_ID_CNV1001,"join begin "+new java.util.Date())
    val notChangeDF = sqlContext.sql(sql.toString)
    logger.info(MESSAGE_ID_CNV1001,"join end "+new java.util.Date())

    val tempDir = getTempDir(dataProcessConfig.fTableName)
    implicit val configuration = sparkContext.hadoopConfiguration
    //删除临时目录
    DFSUtils.deleteDir(tempDir)
    logger.info(MESSAGE_ID_CNV1001,"save begin "+new java.util.Date())
    notChangeDF.unionAll(newDF).select("content").write.text(tempDir)
    logger.info(MESSAGE_ID_CNV1001,"save end "+new java.util.Date())

    //删除目标表数据
    DFSUtils.deleteDir(this.dataProcessConfig.datFileOutputPath)
    DFSUtils.createDir(this.dataProcessConfig.datFileOutputPath)
    //将没有改变的数据转移到目标表目录
    val fileSystem = FileSystem.get(configuration)
    val fileStatusArray = fileSystem.listStatus(new Path(tempDir)).filter(_.getLen > 0)
    var fileIndex = 0
    for (fileStatus <- fileStatusArray) {
      val fileName = "%s/%05d".format(dataProcessConfig.datFileOutputPath,fileIndex)
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
object I2FWithDFOnSpark extends App {
  val config = new DataProcessOnSparkConfig()
  val job = new I2FWithDFOnSparkJob()
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
