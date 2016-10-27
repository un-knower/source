package com.boc.iff

import com.boc.iff.IFFConversion._
import com.boc.iff.model._
import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.fs.{FileSystem, Path}
import java.io.{BufferedInputStream, File, FileInputStream}

import scala.collection.mutable.ArrayBuffer

class I2FOnSparkJob  extends DataProcessOnSparkJob {

  override def processFile = {
    println(this.dataProcessConfig.toString);

    //删除dataProcessConfig.tempDir
    val fields: List[IFFField] = iffMetadata.getBody.fields
    val tableFields = fields.filter(x=>x.filter!="Y")  //
    val primaryFields :List[IFFField]= fields.filter(x=>x.getExpression!="Y") //

    var pkPosition = ArrayBuffer[Int]()
    for(v<-primaryFields){
      pkPosition +=tableFields.indexOf(v)
    }

    val basePk2Map= (x:String) => {
      val rowData = x.split("\\001")
      var key=""
      for(v<-pkPosition){
        key+=rowData(v).trim
      }
      (key,x)
    }


    val newRDD = sparkContext.textFile(this.dataProcessConfig.iffFileInputPath).map(basePk2Map)
    val fullRDDTable = sparkContext.textFile(this.dataProcessConfig.datFileOutputPath).map(basePk2Map)



    val fullRDD1 =  fullRDDTable.leftOuterJoin(newRDD)
    val noChangeRdd = fullRDD1.filter(x=>if(x._2._2.isEmpty) true else false).map(x=>x._2._1)
    noChangeRdd.saveAsTextFile(dataProcessConfig.tempDir)

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
