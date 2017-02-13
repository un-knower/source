package com.context

import com.model.FileInfo
import java.util.List

/**
  * Created by scutlxj on 2017/2/9.
  */
class FileReadStageRequest extends StageRequest{
  var fileInfos:List[FileInfo] = _
}

class FileSaveStageRequest extends StageRequest{
  var cleanTargetPath:Boolean = false
  var fileInfos:List[FileInfo] = _

}
