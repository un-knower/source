package com.model

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
  var filters:List[String] = _

  @BeanProperty
  var groupBy:String = ""

  @BeanProperty
  var resultFilters:List[String] = _


}
