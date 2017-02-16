package com.datahandle

import com.boc.iff.exception.StageInfoErrorException
import com.context.{SqlStageRequest, StageRequest}
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.DataFrame

/**
  * Created by scutlxj on 2017/2/14.
  */
class JoinStageHandle[T<:StageRequest] extends SqlStageHandle[T]{
  override protected def handle(sqlStageRequest: SqlStageRequest): DataFrame = {
    if(sqlStageRequest.inputTables.size()<2){
      logBuilder.error("Stage[%s]--JoinStage inputTable number must be more than two".format(sqlStageRequest.stageId))
      throw StageInfoErrorException("Stage[%s]--JoinStage inputTable number must be more than two".format(sqlStageRequest.stageId))
    }
    if(StringUtils.isEmpty(sqlStageRequest.from)){
      logBuilder.error("Stage[%s]--JoinStage 'form' is required  ".format(sqlStageRequest.stageId))
      throw StageInfoErrorException("Stage[%s]--JoinStage 'form' is required  ".format(sqlStageRequest.stageId))
    }
    super.handle(sqlStageRequest)
  }
}
