package com.datahandle

import com.boc.iff.DFSUtils
import com.context.{FileSaveStageRequest, StageAppContext, StageRequest}
import com.datahandle.load.{HiveFileLoader, ParquetFileLoader, SimpleFileLoader}
import com.datahandle.save.{FileSaver, ParquetFileSaver, TextFileSaver}
import com.model.FileInfo
import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkContext

/**
  * Created by scutlxj on 2017/2/10.
  */
class FileSaveStageHandle[T<:StageRequest] extends StageHandle[T] {

  override def doCommand(stRequest: StageRequest)(implicit appContext:StageAppContext): Unit = {
    val fileStageRequest = stRequest.asInstanceOf[FileSaveStageRequest]
    for(index<-0 until fileStageRequest.inputTables.size()){
      val tableName = fileStageRequest.inputTables.get(index)
      val fileInfo = fileStageRequest.fileInfos.get(index)
      val saver = getSaver(fileInfo)
      saver.save(tableName,fileInfo,fileStageRequest.cleanTargetPath)
    }
  }

  private def getSaver(fileInfo:FileInfo):FileSaver={
    fileInfo.fileType match {
      case "parquet" => new ParquetFileSaver
      case _ => new TextFileSaver
    }
  }


}
