package com.datahandle

import com.context.{FileReadStageRequest, StageAppContext, StageRequest}
import com.datahandle.load.{FileLoader, HiveFileLoader, ParquetFileLoader, SimpleFileLoader}
import com.model.FileInfo

/**
  * Created by scutlxj on 2017/2/9.
  */
class FileReadStageHandle[T<:StageRequest] extends StageHandle[T] {
  override def doCommand(stRequest: StageRequest)(implicit appContext:StageAppContext): Unit = {
    val fileStageRequest = stRequest.asInstanceOf[FileReadStageRequest]
    for(index<-0 until fileStageRequest.fileInfos.size()){
      val fileInfo = fileStageRequest.fileInfos.get(index)
      if(!appContext.tablesMap.containsKey(fileInfo.targetName)) {
        this.getFileLoader(fileInfo).load(fileInfo)
      }
    }

  }

  private def getFileLoader(fileInfo:FileInfo): FileLoader ={
    fileInfo.fileType match {
      case "parquet" => new ParquetFileLoader
      case "hive" => new HiveFileLoader
      case _ => new SimpleFileLoader
    }
  }

}
