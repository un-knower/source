package com.boc.iff.itf

import com.boc.iff.DFSUtils
import com.boc.iff.IFFConversion._
import com.boc.iff.exception.PrimaryKeyMissException
import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{SQLContext, SaveMode}


/**
  * @author www.birdiexx.com
  */
class I2FWithDataFrameOnSparkJob
  extends DataProcessOnSparkJob with Serializable{

  override def processFile = {
    println(this.dataProcessConfig.toString)
    val primaryFields = iffMetadata.getBody.fields.filter(_.primaryKey) //
    if(primaryFields==null||primaryFields.size==0){
        throw PrimaryKeyMissException("Primary Key of table"+dataProcessConfig.fTableName+" is required")
    }

    logger.info("create sqlContext","create sqlContext")
    val sqlContext = new SQLContext(sparkContext)
    val fFableDF = sqlContext.read.parquet(dataProcessConfig.fTableDatFilePath)
    val iTableDF = sqlContext.read.parquet(dataProcessConfig.iTableDatFilePath)
    fFableDF.registerTempTable("fullTB")
    iTableDF.registerTempTable("newTB")
    val sql = new StringBuffer("select f.* from fullTB f left join newTB n on ")
    for(i<-0 until primaryFields.size){
      if(i>0){
        sql.append(" and ")
      }
      sql.append(" f."+primaryFields(i).name+"=n."+primaryFields(i).name)
    }
    sql.append(" where n."+primaryFields(0).name+" is null ")
    logger.info("jop sql:",sql.toString)
    logger.info(MESSAGE_ID_CNV1001,"join begin: "+new java.util.Date())
    val notChangeDF = sqlContext.sql(sql.toString)

    implicit val configuration = sparkContext.hadoopConfiguration
    val tempDir = getTempDir(dataProcessConfig.fTableName)
    //删除临时目录
    DFSUtils.deleteDir(tempDir)

    notChangeDF.unionAll(iTableDF).write.mode(SaveMode.Overwrite).parquet(tempDir)
    //删除目标表数据
    DFSUtils.deleteDir(this.dataProcessConfig.fTableDatFilePath)
    DFSUtils.createDir(this.dataProcessConfig.fTableDatFilePath)

    //将没有改变的数据转移到目标表目录
    val fileSystem = FileSystem.get(configuration)
    val fileStatusArray = fileSystem.listStatus(new Path(tempDir)).filter(_.getLen > 0)
    var fileIndex = 0
    for (fileStatus <- fileStatusArray) {
      val srcPath = fileStatus.getPath
      val fileName = if(srcPath.toString.endsWith(".gz.parquet")){
        fileIndex += 1
        "%s/%05d%s".format(dataProcessConfig.fTableDatFilePath,fileIndex,".gz.parquet")
      }else{
        "%s/%s".format(dataProcessConfig.fTableDatFilePath,srcPath.getName)
      }
      val dstPath = new Path(fileName)
      DFSUtils.moveFile(srcPath, dstPath)
    }
  }
}

/**
 * Spark 程序入口
 *
  * @author www.birdiexx.com
 */
object I2FWithDataFrameOnSparkJob extends App {
  val config = new DataProcessOnSparkConfig()
  val job = new I2FWithDataFrameOnSparkJob()
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

/**
  * @author www.birdiexx.com
  */
