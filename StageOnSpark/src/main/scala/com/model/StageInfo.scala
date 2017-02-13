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

  def getStageRequest(implicit stageAppContext: StageAppContext):StageRequest





}
