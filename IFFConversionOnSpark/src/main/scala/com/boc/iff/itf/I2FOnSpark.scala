package com.boc.iff.itf

import com.boc.iff.DFSUtils
import com.boc.iff.IFFConversion._
import com.boc.iff.model._
import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.fs.{FileSystem, Path}

import scala.collection.mutable.ArrayBuffer

class I2FOnSparkJob  extends DataProcessOnSparkJob with Serializable {

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
      (key,x)
    }
    val newRDD = sparkContext.textFile(this.dataProcessConfig.iffFileInputPath)
    val iRDD = newRDD.map(basePk2Map)
    val fullRDDTable = sparkContext.textFile(this.dataProcessConfig.datFileOutputPath).map(basePk2Map)
    val fullRDD1 =  fullRDDTable.leftOuterJoin(iRDD)
    val noChangeRdd = fullRDD1.filter(x=>if(x._2._2.isEmpty) true else false).map(x=>x._2._1)

    val tempDir = getTempDir(dataProcessConfig.fTableName)
    implicit val configuration = sparkContext.hadoopConfiguration
    //删除临时目录
    DFSUtils.deleteDir(tempDir)
    noChangeRdd.union(newRDD).saveAsTextFile(tempDir)

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



    //val partFuction : (Iterator[String])=>Iterator[(String,String)] ={
      //x=> Iterator(("1","2"))
    //}
    //sparkContext.textFile(this.dataProcessConfig.datFileOutputPath).mapPartitions(partFuction)

    //删除datFileOutputPath
    //move dataProcessConfig.tempDir 到datFileOutputPath
    //move dataProcessConfig.iffFileInputPath 到datFileOutputPath

  }
}

/**
 * Spark 程序入口
 */
object I2FOnSpark extends App {
  val config = new DataProcessOnSparkConfig()
  val job = new I2FOnSparkJob()
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
