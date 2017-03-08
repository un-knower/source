package com.context
import java.util.List

import com.model.{DebugInfo, TableInfo}
/**
  * Created by cvinc on 2016/6/8.
  */
trait StageRequest {
    var stageId:String = ""
    var nextStageId:String = ""
    var inputTables:List[String] = _
    var debugInfo:DebugInfo = _
    var outputTable:TableInfo = _
}
object StageRequest{

}
