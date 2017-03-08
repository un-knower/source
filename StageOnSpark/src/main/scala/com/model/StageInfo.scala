package com.model

import java.util.List

import com.context.{StageAppContext, StageRequest}

import scala.beans.BeanProperty

/**
  * Created by cvinc on 2016/6/8.
  */
abstract class StageInfo extends Serializable {


  @BeanProperty
  var stageId: String = ""
  @BeanProperty
  var stageDesc: String = ""
  @BeanProperty
  var nextStageId: String = ""
  @BeanProperty
  var stageType:String = ""
  @BeanProperty
  var inputTables:List[String] = _
  @BeanProperty
  var debugInfo:DebugInfo = _
  @BeanProperty
  var outPutTable:TableInfo = _


  def getStageRequest(implicit stageAppContext: StageAppContext):StageRequest
}

object StageInfo extends Serializable {
  object StageType extends Enumeration {
    val File = "FILE"
    val Aggregate = "AGGREGATE"
    val Join = "JOIN"
    val Sort = "SORT"
    val Union = "UNION"
    val Transformer = "TRANSFORMER"
  }
}
