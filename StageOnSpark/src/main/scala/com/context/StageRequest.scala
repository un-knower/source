package com.context
import java.util.List
/**
  * Created by cvinc on 2016/6/8.
  */
trait StageRequest {
    var stageId:String = ""
    var nextStageId:String = ""
    var inputTables:List[String] = null
}
object StageRequest{

}
