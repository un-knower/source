package com.datahandle

import com.boc.iff.exception.StageInfoErrorException
import com.context.{SqlStageRequest, StageRequest}
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.DataFrame

/**
  * Created by scutlxj on 2017/2/14.
  */
class SortStageHandle[T<:StageRequest] extends SqlStageHandle[T]{

  override protected def handle(sqlStageRequest: SqlStageRequest): DataFrame = {
    if(sqlStageRequest.inputTables.size()>1){
      logBuilder.error("Stage[%s]--SortStage can only set one inputTable".format(sqlStageRequest.stageId))
      throw StageInfoErrorException("Stage[%s]--SortStage can only set one inputTable".format(sqlStageRequest.stageId))
    }
    if(sqlStageRequest.outPutTable.body==null||sqlStageRequest.outPutTable.body.fields==null){
      sqlStageRequest.outPutTable.body = appContext.getTable(sqlStageRequest.inputTables.get(0)).body
    }
    if(StringUtils.isEmpty(sqlStageRequest.from)){
      sqlStageRequest.from = sqlStageRequest.inputTables.get(0)
    }
    super.handle(sqlStageRequest)
  }

}
