package com.datahandle

import com.boc.iff.DFSUtils
import com.context.{FileSaveStageRequest, StageAppContext, StageRequest}
import com.datahandle.load.{HiveFileLoader, ParquetFileLoader, SimpleFileLoader}
import com.datahandle.save.{FileSaver, ParquetFileSaver, TextFileSaver}
import com.model.FileInfo
import com.model.FileInfo.FileType
import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkContext

/**
  * Created by scutlxj on 2017/2/10.
  */
class FileSaveStageHandle[T<:StageRequest] extends StageHandle[T] {

  override def execute(stRequest: StageRequest): Unit = {
    val fileStageRequest = stRequest.asInstanceOf[FileSaveStageRequest]
    implicit val context:StageAppContext = appContext
    for(index<-0 until fileStageRequest.inputTables.size()){
      val tableName = fileStageRequest.inputTables.get(index)
      val fileInfo = fileStageRequest.fileInfos.get(index)
      logBuilder.info("Save Table[%s] to File[%s]".format(tableName,fileInfo.dataPath))
      val saver = getSaver(fileInfo)
      saver.save(tableName,fileInfo,fileStageRequest.cleanTargetPath)
    }
  }

  private def getSaver(fileInfo:FileInfo):FileSaver={
    fileInfo.fileType match {
      case FileType.PARQUET => new ParquetFileSaver
      case _ => new TextFileSaver
    }
  }


}
