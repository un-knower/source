package com.context

import java.util.List

import com.model.{DeduplicateInfo, TableInfo}

import scala.beans.BeanProperty

/**
  * Created by cvinc on 2016/6/8.
  */
class SqlStageRequest extends StageRequest{

  var logicFilter:String = ""

  var limitFilter:Int = 0

  var sorts:List[String] = _

  var groupBy:String = ""

  var from:String = ""
}


class AggregateStageRequest extends SqlStageRequest

class JoinStageRequest extends SqlStageRequest

class UnionStageRequest extends SqlStageRequest

class TransformerStageRequest extends SqlStageRequest

class SortStageRequest extends SqlStageRequest

class DeduplicateStageRequest extends SqlStageRequest

class LookupStageRequest extends SqlStageRequest{
  @BeanProperty
  var mainTable:String = ""

  @BeanProperty
  var deduplicateInfos:List[DeduplicateInfo] = _
}

class MergeStageRequest extends SqlStageRequest{
  @BeanProperty
  var insertTableInfo:TableInfo = _
}
