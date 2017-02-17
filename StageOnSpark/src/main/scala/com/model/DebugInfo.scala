package com.model

import scala.beans.BeanProperty

/**
  * Created by scutlxj on 2017/2/17.
  */
class DebugInfo extends Serializable{
  @BeanProperty
  var method:String = ""
  @BeanProperty
  var delimiter:String = "|"
  @BeanProperty
  var quote:String = ""
  @BeanProperty
  var limit:Int = 10
  @BeanProperty
  var file:String = ""
}
