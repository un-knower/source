package com.model

import java.util.List

import com.context._
import com.model.StageInfo.StageType

import scala.beans.BeanProperty

/**
  * Created by scutlxj on 2017/1/20.
  */
class SqlStageInfo extends StageInfo{




  @BeanProperty
  var from:String = ""

  @BeanProperty
  var logicFilter:String = ""

  @BeanProperty
  var limitFilter:Int = 0

  @BeanProperty
  var groupBy:String = ""

  @BeanProperty
  var sorts:List[String] = _

  def getStageRequest(implicit stageAppContext: StageAppContext):StageRequest={
    val sqlStageRequest:SqlStageRequest = stageType match {
      case StageType.Aggregate => new AggregateStageRequest
      case StageType.Join => new JoinStageRequest
      case StageType.Sort => new SortStageRequest
      case StageType.Transformer => new TransformerStageRequest
      case StageType.Union => new UnionStageRequest
    }
    sqlStageRequest.outputTable = this.outPutTable
    sqlStageRequest.inputTables = this.inputTables
    sqlStageRequest.sorts = this.sorts
    sqlStageRequest.from = this.from
    sqlStageRequest.stageId = this.stageId
    sqlStageRequest.nextStageId = this.nextStageId
    sqlStageRequest.groupBy = this.groupBy
    sqlStageRequest.limitFilter = this.limitFilter
    sqlStageRequest.logicFilter = this.logicFilter
    sqlStageRequest.debugInfo = debugInfo
    sqlStageRequest
  }

}
