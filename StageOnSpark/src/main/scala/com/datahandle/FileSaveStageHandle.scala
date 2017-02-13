package com.datahandle

import com.boc.iff.DFSUtils
import com.context.{FileSaveStageRequest, StageAppContext, StageRequest}
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
    }
  }


}
