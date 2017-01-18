package com.model

import com.boc.iff.model.IFFSection

import scala.beans.BeanProperty

/**
  * Created by cvinc on 2016/6/8.
  */
class StageInput extends Serializable {

  @BeanProperty
  var sourceCharset: String = null

  @BeanProperty
  var targetSchema: String = null

  @BeanProperty
  var targetTable: String = null

  @BeanProperty
  var header: IFFSection = null

  @BeanProperty
  var body: IFFSection = null

  @BeanProperty
  var footer: IFFSection = null

  @BeanProperty
  var srcSystem: String = null

  @BeanProperty
  var fixedLength: String = null

  @BeanProperty
  var srcSeparator: String = null

}
