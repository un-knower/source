package com.model

import com.context.{FileReadStageRequest, FileSaveStageRequest, StageAppContext, StageRequest,FileStageRequest}

import scala.beans.BeanProperty

/**
  * Created by scutlxj on 2017/2/8.
  */
class FileStageInfo extends StageInfo{

  object OperationType extends Enumeration {
    val READ = "READ"
    val APPEND = "APPEND"
    val OVERWRITE = "OVERWRITE"
  }

  @BeanProperty
  var operationType:String = ""
  @BeanProperty
  var fileInfos:java.util.List[FileInfo] = _


  def getStageRequest(implicit stageAppContext: StageAppContext):StageRequest={
    val request:FileStageRequest = operationType match {
      case OperationType.READ =>
        new FileReadStageRequest
      case OperationType.APPEND =>
        val re = new FileSaveStageRequest
        re.cleanTargetPath = false
        re
      case OperationType.OVERWRITE =>
        val re = new FileSaveStageRequest
        re.cleanTargetPath = true
        re
    }
    request.fileInfos = fileInfos
    request.inputTables = this.inputTables
    request.debugInfo = debugInfo
    request.stageId = stageId
    request.outputTable = this.outPutTable
    request
  }

}

class FileInfo{
  @BeanProperty
  var metadataFileEncoding: String = "UTF-8"                                //XML 描述文件编码
  @BeanProperty
  var sourceCharset:String = "UTF-8"
  @BeanProperty
  var fileType:String = ""
  @BeanProperty
  var dataPath:String = ""
  @BeanProperty
  var xmlPath:String = ""
  @BeanProperty
  var targetSeparator:String = "\001"
  @BeanProperty
  var dataLineEndWithSeparatorF:Boolean = false
  @BeanProperty
  var singleFileFlag:String = "N"
}

object FileInfo{
  object FileType extends Enumeration {
    val HIVE = "HIVE"
    val PARQUET = "PARQUET"
    val UNFIXLENGTH = "UNFIXLENGTH"
    val FIXLENGTH = "FIXLENGTH"
  }
}
