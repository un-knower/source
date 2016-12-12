package com.boc.iff.history

import java.util

import com.boc.iff.DFSUtils
import com.boc.iff.model.{IFFField, IFFMetadata}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path, PathFilter}
import org.apache.spark.SparkContext
import org.apache.spark.sql.types.{DataTypes, StructField}
import org.apache.spark.sql.{DataFrame, Row, SQLContext}

import scala.collection.mutable.ArrayBuffer

/**
  * Created by scutlxj on 2016/12/7.
  */
trait DataReader {



  def getData(filePath:String,iffMetadata: IFFMetadata,sqlContext: SQLContext,fieldSeparator:String):DataFrame={
    ???
  }

}

trait TextDateReader extends DataReader{
  override def getData(filePath:String,iffMetadata: IFFMetadata,sqlContext: SQLContext,fieldSeparator:String):DataFrame={
    val sparkContext = sqlContext.sparkContext
    val fileSystem = FileSystem.get(sparkContext.hadoopConfiguration)
    var dataFrame:DataFrame = null
    if(fileSystem.exists(new Path(filePath))) {
      val fs = fileSystem.listStatus(new Path(filePath))
      if(fs!=null&&fs.length>0) {
        val rdd = sparkContext.textFile(filePath)
        val fields: List[IFFField] = iffMetadata.getBody.fields.filter(!_.filter)
        val basePk2Map = (x: String) => {
          val rowData = x.split(fieldSeparator)
          val array = new ArrayBuffer[String]
          for (v <- rowData) {
            array += v
          }
          Row.fromSeq(array)
        }
        val structFields = new util.ArrayList[StructField]()
        for (f <- fields) {
          structFields.add(DataTypes.createStructField(f.name, DataTypes.StringType, true))
        }
        val structType = DataTypes.createStructType(structFields)
        val rddN = rdd.map(basePk2Map)
        dataFrame = sqlContext.createDataFrame(rddN, structType)
      }
    }
    dataFrame
  }
}

trait ParquetDateReader extends DataReader{
  override def getData(filePath:String,iffMetadata: IFFMetadata,sqlContext: SQLContext,fieldSeparator:String):DataFrame={
    val sparkContext = sqlContext.sparkContext
    val fileSystem = FileSystem.get(sparkContext.hadoopConfiguration)
    var dataFrame:DataFrame = null
    if(fileSystem.exists(new Path(filePath))) {
      val pathFilter = new PathFilter {
        override def accept(path: Path): Boolean = {
          !(path.getName.equals("_metadata")&&path.getName.equals("_common_metadata"))
        }
      }
      val fs = fileSystem.listStatus(new Path(filePath),pathFilter)
      if(fs!=null&&fs.length>0) {
        dataFrame = sqlContext.read.parquet(filePath)
      }
    }
    dataFrame
  }


}

trait DataWriter{
  def writeData(df:DataFrame,filePath:String,fieldSeparator:String): Unit={

  }

  def saveToTarget(sourcePath: String,targetPath:String,sparkContext:SparkContext): Unit={

  }
}

trait TextDataWriter extends DataWriter{
  override def writeData(df:DataFrame,filePath:String,fieldSeparator:String): Unit={
    val rdd = df.rdd.map((row)=>row.toSeq.reduceLeft(_+fieldSeparator+_))
    rdd.saveAsTextFile(filePath)
  }

  override def saveToTarget(sourcePath: String,targetPath:String,sparkContext:SparkContext): Unit = {
    val fileSystem = FileSystem.get(sparkContext.hadoopConfiguration)
    //移除目标目录中打开的记录
    val openSts = fileSystem.listStatus(new Path(sourcePath),new PathFilter {
      override def accept(path: Path): Boolean = {
        path.getName.endsWith("OPEN")
      }
    })
    for(s<-openSts){
      fileSystem.delete(s.getPath, true)
    }

    var fileIndex = 0
    val appId = sparkContext.applicationId
    var targetFilePath:Path = null
    //转移close文件
    val closePath = new Path("%s/%s".format(sourcePath,"close"))
    if(fileSystem.exists(closePath)) {
      val closeFileSts = fileSystem.listStatus(new Path("%s/%s".format(sourcePath, "close")))
      for (cs <- closeFileSts.filter(_.getLen > 0)) {
        targetFilePath = new Path("%s/%s_%05d_%s".format(targetPath, appId, fileIndex, "CLOSE"))
        DFSUtils.moveFile(fileSystem, cs.getPath, targetFilePath)
        fileIndex += 1
      }
    }

    //转移open文件
    fileIndex=0
    val openFileSts = fileSystem.listStatus(new Path("%s/%s".format(sourcePath,"open")))
    for(cs<-openFileSts.filter(_.getLen>0)){
      targetFilePath = new Path("%s/%s_%05d_%s".format(targetPath,appId,fileIndex,"OPEN"))
      DFSUtils.moveFile(fileSystem,cs.getPath,targetFilePath)
    }
  }
}

trait ParquetDataWriter extends DataWriter{
  override def writeData(df:DataFrame,filePath:String,fieldSeparator:String): Unit={
    df.write.parquet(filePath)
  }

  override def saveToTarget(sourcePath: String,targetPath:String,sparkContext:SparkContext): Unit = {
    val fileSystem = FileSystem.get(sparkContext.hadoopConfiguration)
    //移除目标目录中打开的记录
    val openSts = fileSystem.listStatus(new Path(sourcePath),new PathFilter {
      override def accept(path: Path): Boolean = {
        path.getName.endsWith("OPEN")
      }
    })
    for(s<-openSts){
      fileSystem.delete(s.getPath, true)
    }
    //转移Metadata文件
    val targetCommonMetadataPath = new Path("%s/%s".format(targetPath,"_common_metadata"))
    val targetMetadataPath = new Path("%s/%s".format(targetPath,"_metadata"))
    if(fileSystem.exists(targetMetadataPath)){
      fileSystem.delete(targetCommonMetadataPath, true)
      fileSystem.delete(targetMetadataPath, true)
    }
    val sourceCommonMetadataPath = new Path("%s/%s/%s".format(sourcePath,"open","_common_metadata"))
    val sourceMetadataPath = new Path("%s/%s/%s".format(sourcePath,"open","_metadata"))
    if(fileSystem.exists(sourceCommonMetadataPath)){
      DFSUtils.moveFile(fileSystem,sourceCommonMetadataPath,targetCommonMetadataPath)
      DFSUtils.moveFile(fileSystem,sourceMetadataPath,targetMetadataPath)
    }

    val pathFilter = new PathFilter {
      override def accept(path: Path): Boolean = {
        !(path.getName.equals("_metadata")&&path.getName.equals("_common_metadata"))
      }
    }

    var fileIndex = 0
    val appId = sparkContext.applicationId
    var targetFilePath:Path = null
    //转移close文件
    val closePath = new Path("%s/%s".format(sourcePath,"close"))
    if(fileSystem.exists(closePath)) {
      val closeFileSts = fileSystem.listStatus(closePath, pathFilter)
      for (cs <- closeFileSts.filter(_.getLen > 0)) {
        targetFilePath = new Path("%s/%s_%05d_%s".format(targetPath, appId, fileIndex, "CLOSE"))
        DFSUtils.moveFile(fileSystem, cs.getPath, targetFilePath)
        fileIndex += 1
      }
    }

    //转移open文件
    fileIndex=0
    val openFileSts = fileSystem.listStatus(new Path("%s/%s".format(sourcePath,"open")),pathFilter)
    for(cs<-openFileSts.filter(_.getLen>0)){
      targetFilePath = new Path("%s/%s_%05d_%s".format(targetPath,appId,fileIndex,"OPEN"))
      DFSUtils.moveFile(fileSystem,cs.getPath,targetFilePath)
    }
  }
}
