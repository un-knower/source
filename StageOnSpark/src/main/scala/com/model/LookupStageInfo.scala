package com.model

import scala.beans.BeanProperty
import java.util.List

import com.context.{LookupStageRequest, StageAppContext, StageRequest}

/**
  * Created by scutlxj on 2017/4/10.
  */
class LookupStageInfo extends SqlStageInfo{

  @BeanProperty
  var mainTable:String = ""

  @BeanProperty
  var deduplicateInfos:List[DeduplicateInfo] = _

  override def getStageRequest(implicit stageAppContext: StageAppContext):StageRequest={
    val request = (super.getStageRequest).asInstanceOf[LookupStageRequest]
    request.mainTable = this.mainTable
    request.deduplicateInfos = this.deduplicateInfos
    request
  }


}

class DeduplicateInfo{
  @BeanProperty
  var tableName:String = ""

  @BeanProperty
  var keys:List[String] = _

  @BeanProperty
  var sorts:List[String] = _
}

