package com.model

import com.boc.iff.model.IFFSection
import org.apache.spark.sql.DataFrame

import scala.beans.BeanProperty

/**
  * Created by scutlxj on 2017/1/20.
  */
class TableInfo {

  var loadFlag:Boolean = false

  @BeanProperty
  var sourceCharset: String = null

  @BeanProperty
  var sourceDataPath: String = null

  @BeanProperty
  var sourceFileType: String = null

  @BeanProperty
  var targetName: String = null

  @BeanProperty
  var srcSystem: String = null

  @BeanProperty
  var fixedLength: String = null

  @BeanProperty
  var srcSeparator: String = null

  @BeanProperty
  var header: IFFSection = null

  @BeanProperty
  var body: IFFSection = null

  @BeanProperty
  var footer: IFFSection = null


}
