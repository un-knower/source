package com.datahandle

import com.boc.iff.exception.StageInfoErrorException
import com.context.{SqlStageRequest, StageRequest}
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.DataFrame

/**
  * Created by scutlxj on 2017/2/14.
  */
class AggregateStageHandle[T<:StageRequest] extends SqlStageHandle[T]{
  override protected def handle(sqlStageRequest: SqlStageRequest): DataFrame = {
    if(sqlStageRequest.inputTables.size()>1){
      throw StageInfoErrorException("Stage[%s]--AggregateStage can only set one inputTable ".format(sqlStageRequest.stageId))
    }
    if(StringUtils.isEmpty(sqlStageRequest.from)){
      sqlStageRequest.from = sqlStageRequest.inputTables.get(0)
    }
    super.handle(sqlStageRequest)
  }
}
