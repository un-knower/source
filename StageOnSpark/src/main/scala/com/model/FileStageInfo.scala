package com.model

import com.context.{FileReadStageRequest, FileSaveStageRequest, StageAppContext, StageRequest}

import scala.beans.BeanProperty

/**
  * Created by scutlxj on 2017/2/8.
  */
class FileStageInfo extends StageInfo{

  object OperationType extends Enumeration {
    val READ = "READ"
    val APPEND = "APPEND"
    val OVERRIDE = "OVERRIDE"
  }

  @BeanProperty
  var operationType:String = ""
  @BeanProperty
  var fileInfos:java.util.List[FileInfo] = _

  def getStageRequest(implicit stageAppContext: StageAppContext):StageRequest={
    operationType match {
      case OperationType.READ =>
        val request = new FileReadStageRequest
        request.fileInfos = fileInfos
        request
      case OperationType.APPEND =>
        val request = new FileSaveStageRequest
        request.fileInfos = fileInfos
        request.inputTables = this.inputTables
        request.cleanTargetPath = false
        request
      case OperationType.OVERRIDE =>
        val request = new FileSaveStageRequest
        request.fileInfos = fileInfos
        request.inputTables = this.inputTables
        request.cleanTargetPath = true
        request
    }

  }

}

class FileInfo{
  @BeanProperty
  var metadataFileEncoding: String = "UTF-8"                                //XML 描述文件编码
  @BeanProperty
  var targetName:String = ""
  @BeanProperty
  var fileType:String = ""
  @BeanProperty
  var dataPath:String = ""
  @BeanProperty
  var xmlPath:String = ""
  @BeanProperty
  var targetSeparator:String = ""
}
