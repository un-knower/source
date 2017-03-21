package com.datahandle.save
import com.boc.iff.DFSUtils
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}
import org.apache.spark.sql.{DataFrame, Row}

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, ListBuffer}

/**
  * Created by scutlxj on 2017/2/10.
  */
class TextFileSaver extends FileSaver{
  override protected def saveDataFrame(path:String,df: DataFrame): Unit = {
    val targetSeparator = fileInfo.targetSeparator
    val rowToString = (x:Iterator[Row]) => {
      val result:ListBuffer[String] = new ListBuffer[String]
      val str = new StringBuffer()
      val separatorLength = targetSeparator.length
      while(x.hasNext) {
        str.setLength(0)
        for (v <- x.next.toSeq) {
          str.append(v).append(targetSeparator)
        }
        str.setLength(str.length()-separatorLength)
        result+=str.toString
      }
      result.iterator
    }
    df.rdd.save.mapPartitions(rowToString).saveAsTextFile(path)
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
