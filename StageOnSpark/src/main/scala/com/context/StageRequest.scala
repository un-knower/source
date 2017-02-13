package com.context
import java.util.List
/**
  * Created by cvinc on 2016/6/8.
  */
trait StageRequest {
    var stageName:String = ""
    var nextStage:String = ""
    var inputTables:List[String] = null
}
object StageRequest{

}
