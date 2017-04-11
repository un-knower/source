package com.model

import com.context.{LookupStageRequest, MergeStageRequest, StageAppContext, StageRequest}

import scala.beans.BeanProperty

/**
  * Created by scutlxj on 2017/4/10.
  */
class MergeStageInfo extends SqlStageInfo {

  @BeanProperty
  var insertTableInfo:TableInfo = _

  override def getStageRequest(implicit stageAppContext: StageAppContext):StageRequest={
    val request = (super.getStageRequest).asInstanceOf[MergeStageRequest]
    request.insertTableInfo = this.insertTableInfo
    request
  }

}
