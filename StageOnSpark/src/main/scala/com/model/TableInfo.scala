package com.model

import com.boc.iff.model.IFFMetadata

import scala.beans.BeanProperty

/**
  * Created by scutlxj on 2017/1/20.
  */
class TableInfo extends IFFMetadata{

  var cacheFlag:Boolean = false

  @BeanProperty
  var targetName: String = ""

  @BeanProperty
  var dataLineEndWithSeparatorF:Boolean = false

  @BeanProperty
  var lengthOfLineEnd:Int = 1

  @BeanProperty
  var fileEOFPrefix:String = "|||||"






}
