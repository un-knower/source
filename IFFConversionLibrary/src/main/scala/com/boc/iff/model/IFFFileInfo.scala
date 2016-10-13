package com.boc.iff.model

import scala.beans.{BeanProperty, BooleanBeanProperty}

/**
  * Created by cvinc on 2016/6/8.
  */
class IFFFileInfo extends Serializable{

  @BooleanBeanProperty
  var gzip: Boolean = false

  @BooleanBeanProperty
  var host: Boolean = false

  @BeanProperty
  var recordLength: Int = 0

  @BeanProperty
  var iffId: String = null

  @BeanProperty
  var fileName: String = null

  @BeanProperty
  var fileLength: Long = 0

  @BooleanBeanProperty
  var dir: Boolean = false
}
