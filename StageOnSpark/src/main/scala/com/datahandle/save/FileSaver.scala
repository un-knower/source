package com.datahandle.save

import com.boc.iff.DFSUtils
import com.config.SparkJobConfig
import com.context.StageAppContext
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

  def save(inputTable:String,fileInfo:FileInfo,cleanTargetPath:Boolean)(implicit stageAppContext: StageAppContext): Unit ={
    sparkContext = stageAppContext.sparkContext
    tableInfo = stageAppContext.getTable(inputTable)
    jobConfig = stageAppContext.jobConfig
    this.fileInfo = fileInfo
    val tmpPath = getTempPath(inputTable)
    val df = stageAppContext.getDataFrame(tableInfo)
    implicit val hadoopConfig = sparkContext.hadoopConfiguration
    DFSUtils.deleteDir(tmpPath)
    try{
      saveDataFrame(tmpPath,df)
      if(cleanTargetPath)cleanPath(fileInfo.dataPath)
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

  protected def saveToTargetPath(tempPath:String,targetPath:String):Unit={
    val fileSystem = FileSystem.get(sparkContext.hadoopConfiguration)
    val sourceFilePath = new Path(tempPath)
    val fileStatus = fileSystem.getFileStatus(sourceFilePath)
    val fileStatusStrack:mutable.Stack[FileStatus] = new mutable.Stack[FileStatus]()
    fileStatusStrack.push(fileStatus)
    var index:Int = 0;
    while(!fileStatusStrack.isEmpty){
      val fst = fileStatusStrack.pop()
      if(fst.isDirectory){
        val fileStatusS = fileSystem.listStatus(fst.getPath)
        for(f<-fileStatusS){
          fileStatusStrack.push(f)
        }
      }else if(fst.getLen>0){
        val fileName =  "%s/%s-%03d".format(targetPath,sparkContext.applicationId, index)
        val srcPath = fst.getPath
        val dstPath = new Path(fileName)
        DFSUtils.moveFile(fileSystem,srcPath, dstPath)
        index+=1
      }
    }
  }



}
