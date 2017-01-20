package com.model

import com.boc.iff.model.{IFFField, IFFSection}

import scala.beans.BeanProperty

/**
  * Created by cvinc on 2016/6/8.
  */
class BatchInfo extends Serializable {

  var tables: List[TableInfo] = Nil
  var stages: List[StageInfo] = Nil

}
