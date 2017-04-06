package com.datahandle.save
import java.io.FileInputStream
import java.util.Properties

import com.boc.iff.{CommonFieldConvertorContext, DFSUtils, ECCLogger}
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}
import org.apache.spark.sql.{DataFrame, Row}

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, ListBuffer}

/**
  * Created by scutlxj on 2017/2/10.
  */
class TextFileSaver extends FileSaver{
  override protected def saveDataFrame(path:String,df: DataFrame): Unit = {
    import com.boc.iff.CommonFieldConvertorContext._
    implicit val fieldConvertorContext = new CommonFieldConvertorContext(null, null, null)
    val targetSeparator = fileInfo.targetSeparator
    val fields = tableInfo.getBody.fields
    val rowToString = (x:Row)=> {
      val sb = new StringBuffer()
      for(i<-0 until fields.size){
        if(i>0)sb.append(targetSeparator)
        sb.append(fields(i).objectToString(x(i)))
      }
      sb.toString
    }
    df.rdd.map(rowToString).saveAsTextFile(path)
  }

  override protected def saveToTargetPath(tempPath:String,targetPath:String):Unit={
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
