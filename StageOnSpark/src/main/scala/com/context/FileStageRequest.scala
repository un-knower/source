package com.context

import com.model.FileInfo
import java.util.List

/**
  * Created by scutlxj on 2017/2/9.
  */
class FileStageRequest extends StageRequest{
  var fileInfos:List[FileInfo] = _
}

class FileReadStageRequest extends FileStageRequest{

}

class FileSaveStageRequest extends FileStageRequest{
  var cleanTargetPath:Boolean = false

}
