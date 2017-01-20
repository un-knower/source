package com.model

import com.boc.iff.model.IFFSection

import scala.beans.BeanProperty

/**
  * Created by cvinc on 2016/6/8.
  */
class StageInfo extends Serializable {

  @BeanProperty
  var stageName: String = null
  @BeanProperty
  var stageDesc: String = null
  @BeanProperty
  var nextStageName: String = null
  @BeanProperty
  var inputTables:List[String] = null



}
