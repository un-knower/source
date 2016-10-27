package com.boc.iff

import com.boc.iff.IFFConversion._
import com.boc.iff.model._
import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.fs.{FileSystem, Path}
import java.io.{BufferedInputStream, File, FileInputStream}
import java.sql.SQLClientInfoException

import com.boc.iff.exception.PrimaryKeyMissException

class I2FWithDataFrameOnSparkJob
  extends DataProcessOnSparkJob {

  override def processFile = {
    println(this.dataProcessConfig.toString);
    //删除dataProcessConfig.tempDir
    val primaryFields = iffMetadata.getBody.fields.filter(_.getPrimaryKey.equals("Y")) //
    if(primaryFields==null||primaryFields.size==0){
        throw PrimaryKeyMissException("Primary Key of table"+dataProcessConfig.fTableName+" is required")
    }
    val sqlContext = new org.apache.spark.sql.SQLContext(sparkContext)
    val fFableDF = sqlContext.table(dataProcessConfig.dbName+"."+dataProcessConfig.fTableName)
    val iTableDF = sqlContext.table(dataProcessConfig.dbName+"."+dataProcessConfig.iTableName)
    fFableDF.registerTempTable("full")
    iTableDF.registerTempTable("new")
    val sql = new StringBuffer("select g1.* from full f left join new n on ")
    for(i<-0 until primaryFields.size){
      if(i>0){
        sql.append(" and ")
      }
      sql.append(" f."+primaryFields(i).name+"=n."+primaryFields(i).name)
    }
    sql.append(" where f."+primaryFields(0).name+" is null ")
    val notChangeDF = sqlContext.sql(sql.toString)
    val newFullRDD = notChangeDF.unionAll(iTableDF).rdd.map(row=>row.toSeq.reduceLeft(_+this.fieldDelimiter+_))
    //newFullDF.write.insertInto(dataProcessConfig.dbName+"."+dataProcessConfig.fTableName)
    val tempDir = getTempDir(dataProcessConfig.fTableName)
    implicit val configuration = sparkContext.hadoopConfiguration
    DFSUtils.deleteDir(tempDir)
    newFullRDD.saveAsTextFile(tempDir)
    val fileSystem = FileSystem.get(configuration)
    val fileStatusArray = fileSystem.listStatus(new Path(tempDir)).filter(_.getLen > 0)
    for (fileStatusIndex <- fileStatusArray.indices.view) {
      val fileStatus = fileStatusArray(fileStatusIndex)
      val fileName = "%s/%05d".format(dataProcessConfig.datFileOutputPath, fileStatusIndex)
      val srcPath = fileStatus.getPath
      val dstPath = new Path(fileName)
      DFSUtils.moveFile(srcPath, dstPath)
    }

  }
}

/**
 * Spark 程序入口
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
