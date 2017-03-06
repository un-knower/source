package com.model

import scala.beans.BeanProperty

/**
  * Created by cvinc on 2016/6/8.
  */
class BatchInfo extends Serializable {

  @BeanProperty
  var batchJobName:String = ""
  @BeanProperty
  var stages: java.util.List[StageInfo] = _
  @BeanProperty
  var batchArgNames: java.util.List[String] = _

}
