package com.datahandle

import com.boc.iff.exception.{BaseException, StageHandleException, StageInfoErrorException}
import com.context.{FileReadStageRequest, StageAppContext, StageRequest}
import com.datahandle.load.{FileLoader, HiveFileLoader, ParquetFileLoader, SimpleFileLoader}
import com.model.FileInfo
import com.model.FileInfo.FileType

/**
  * Created by scutlxj on 2017/2/9.
  */
class FileReadStageHandle[T<:StageRequest] extends StageHandle[T] {
  override def execute(stRequest: StageRequest): Unit = {
    implicit val context = appContext
    val fileStageRequest = stRequest.asInstanceOf[FileReadStageRequest]
    for(index<-0 until fileStageRequest.fileInfos.size()){
      val fileInfo = fileStageRequest.fileInfos.get(index)
      if(!context.checkTableExist(fileInfo.targetName)) {
        try {
          this.getFileLoader(fileInfo).load(fileInfo)
        }catch {
          case e:StageInfoErrorException => throw new StageInfoErrorException("Stage[%s]-".format(stRequest.stageId)+e.message)
          case e:StageHandleException => throw new StageHandleException("Stage[%s]-".format(stRequest.stageId)+e.message)
          case t:Throwable => throw t
        }
      }
    }

  }

  private def getFileLoader(fileInfo:FileInfo): FileLoader ={
    fileInfo.fileType match {
      case FileType.PARQUET => new ParquetFileLoader
      case FileType.HIVE  => new HiveFileLoader
      case _ => new SimpleFileLoader
    }
  }

}
