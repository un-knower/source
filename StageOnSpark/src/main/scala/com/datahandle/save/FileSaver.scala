package com.datahandle.save

import com.boc.iff.DFSUtils
import com.config.SparkJobConfig
import com.context.StageAppContext
import com.log.LogBuilder
import com.model.{FileInfo, TableInfo}
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}
import org.apache.spark.SparkContext
import org.apache.spark.sql.DataFrame

import scala.collection.mutable

/*
  * Created by scutlxj on 2017/2/10.
  */
abstract class FileSaver extends Serializable{
  var sparkContext:SparkContext = _
  var fileInfo:FileInfo = _
  var jobConfig:SparkJobConfig = _
  var tableInfo:TableInfo = _
  var repartitionNumber:Int = _

  var logBuilder:LogBuilder = _

  def save(inputTable:String,fileInfo:FileInfo,cleanTargetPath:Boolean)(implicit stageAppContext: StageAppContext): Unit ={
    sparkContext = stageAppContext.sparkContext
    tableInfo = stageAppContext.getTable(inputTable)
    jobConfig = stageAppContext.jobConfig
    this.fileInfo = fileInfo
    logBuilder = stageAppContext.constructLogBuilder()
    val tmpPath = getTempPath(inputTable)
    val df = if("Y".equals(fileInfo.singleFileFlag)) stageAppContext.getDataFrame(tableInfo).repartition(1) else stageAppContext.getDataFrame(tableInfo)
    implicit val hadoopConfig = sparkContext.hadoopConfiguration
    DFSUtils.deleteDir(tmpPath)
    try{
      saveDataFrame(tmpPath,df)
      if(cleanTargetPath)cleanPath(fileInfo.dataPath)
      DFSUtils.createDir(fileInfo.dataPath)
      saveToTargetPath(tmpPath,fileInfo.dataPath)
    }finally {
      DFSUtils.deleteDir(tmpPath)
    }

  }

  protected def saveDataFrame(path:String,df:DataFrame):Unit

  protected def cleanPath(path:String):Unit={
    implicit val hadoopConfig = sparkContext.hadoopConfiguration
    DFSUtils.deleteDir(path)
    DFSUtils.createDir(path)
  }

  protected def getTempPath(inputTable:String):String={
    "%s/%s".format(jobConfig.tempDir,inputTable)
  }

  protected def saveToTargetPath(tempPath:String,targetPath:String):Unit



}
