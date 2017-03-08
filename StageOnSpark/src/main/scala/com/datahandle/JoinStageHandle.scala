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

  override protected def fillOutPutTable(sqlStageRequest: SqlStageRequest):Unit = {
    for(f<-sqlStageRequest.outputTable.body.fields){
      if(f.typeInfo==null){
        val cols =StringUtils.split(f.fieldExpression,".")
        val sourceTableInfo = appContext.getTable(cols(0))
        val sourceField = sourceTableInfo.getBody.getFieldByName(cols(1))
        if(sourceField==null){
          logBuilder.error("Stage[%s]--Sql type of %s is required ".format(sqlStageRequest.stageId,f.name))
          throw StageInfoErrorException("Stage[%s]--Sql type of %s is required ".format(sqlStageRequest.stageId,f.name))
        }
        f.typeInfo = sourceField.typeInfo
      }
    }
  }
}
