package com.model

import java.util.List

import com.context.{StageAppContext, StageRequest}

import scala.beans.BeanProperty

/**
  * Created by scutlxj on 2017/1/20.
  */
class SqlStageInfo extends StageInfo{


  @BeanProperty
  var outPutTable:TableInfo = _

  @BeanProperty
  var from:String = ""

  @BeanProperty
  var logicFilter:String = ""

  @BeanProperty
  var limitFilter:Int = 0

  @BeanProperty
  var groupBy:String = ""

  @BeanProperty
  var sorts:List[String] = _

  def getStageRequest(implicit stageAppContext: StageAppContext):StageRequest={
    null
  }

}
