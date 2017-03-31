package com.datahandle

import com.boc.iff.exception.StageInfoErrorException
import com.context.{SqlStageRequest, StageRequest}
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.DataFrame
import org.apache.spark.storage.StorageLevel

/**
  * Created by scutlxj on 2017/2/14.
  */
class SortStageHandle[T<:StageRequest] extends SqlStageHandle[T]{

  override protected def handle(sqlStageRequest: SqlStageRequest): DataFrame = {
    if(sqlStageRequest.inputTables.size()>1){
      logBuilder.error("Stage[%s]--SortStage can only set one inputTable".format(sqlStageRequest.stageId))
      throw StageInfoErrorException("Stage[%s]--SortStage can only set one inputTable".format(sqlStageRequest.stageId))
    }
    if(sqlStageRequest.outputTable.body==null||sqlStageRequest.outputTable.body.fields==null){
      sqlStageRequest.outputTable.body = appContext.getTable(sqlStageRequest.inputTables.get(0)).body
    }
    var inputDF = appContext.getDataFrame(sqlStageRequest.inputTables.get(0))
    if(StringUtils.isNotEmpty(sqlStageRequest.logicFilter)) {
      inputDF.persist(StorageLevel.MEMORY_AND_DISK)
      inputDF = inputDF.filter(sqlStageRequest.logicFilter)
      val newTableName = sqlStageRequest.inputTables.get(0) + "Sort_Tmp"
      inputDF.registerTempTable(newTableName)
      sqlStageRequest.from = newTableName
    }
    //提取结果集数量
    val df = if(sqlStageRequest.limitFilter>0){
      super.handle(sqlStageRequest).limit(sqlStageRequest.limitFilter)
    }else{
      super.handle(sqlStageRequest)
    }
    df
  }

  override def filterDF(sqlStageRequest: SqlStageRequest,df:DataFrame) = df

}
