package com.datahandle.save

import com.boc.iff.DFSUtils
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}
import org.apache.spark.sql.{DataFrame, Row}

import scala.collection.mutable

/**
  * Created by scutlxj on 2017/2/10.
  */
class ParquetFileSaver extends FileSaver{
  override protected def saveDataFrame(path:String,df: DataFrame): Unit = {
    df.write.parquet(path)
  }

  override protected def saveToTargetPath(tempPath:String,targetPath:String):Unit={
    val fileSystem = FileSystem.get(sparkContext.hadoopConfiguration)
    val sourceFilePath = new Path(tempPath)
    val fileStatus = fileSystem.getFileStatus(sourceFilePath)
    val fileStatusStrack:mutable.Stack[FileStatus] = new mutable.Stack[FileStatus]()
    fileStatusStrack.push(fileStatus)
    var index:Int = 0;
    var commonMetadataFlag = false
    var metadataFlag = false
    while(!fileStatusStrack.isEmpty){
      val fst = fileStatusStrack.pop()
      if(fst.isDirectory){
        val fileStatusS = fileSystem.listStatus(fst.getPath)
        for(f<-fileStatusS){
          fileStatusStrack.push(f)
        }
      }else if(fst.getLen>0){
        val srcPath = fst.getPath
        if(srcPath.toString.endsWith("_common_metadata")) {
          if(!commonMetadataFlag) {
            val fileName =  "%s/%s".format(targetPath,"_common_metadata")
            val dstPath = new Path(fileName)
            if(!fileSystem.exists(dstPath)) {
              DFSUtils.moveFile(fileSystem, srcPath, dstPath)
            }
            commonMetadataFlag = true
          }
        }else if(srcPath.toString.endsWith("_metadata")) {
          if(!metadataFlag) {
            val fileName =  "%s/%s".format(targetPath,"_metadata")
            val dstPath = new Path(fileName)
            if(!fileSystem.exists(dstPath)) {
              DFSUtils.moveFile(fileSystem, srcPath, dstPath)
            }
            metadataFlag = true
          }
        }else{
          val fileName =  "%s/%s-%03d%s".format(targetPath,sparkContext.applicationId, index,".gz.parquet")
          val dstPath = new Path(fileName)
          DFSUtils.moveFile(fileSystem,srcPath, dstPath)
          index+=1
        }
      }
    }
  }

}
