package com.datahandle
import com.boc.iff.exception.StageInfoErrorException
import com.boc.iff.model.IFFSection
import com.context.{SqlStageRequest, StageRequest}
import com.model.TableInfo
import org.apache.spark.sql.DataFrame

/**
  * Created by scutlxj on 2017/2/14.
  */
class UnionStageHandle[T<:StageRequest] extends SqlStageHandle[T]{
  override protected def handle(sqlStageRequest: SqlStageRequest): DataFrame = {
    if(sqlStageRequest.inputTables.size()<2){
      logBuilder.error("Stage[%s]--UnionStage inputTable number must be more than two".format(sqlStageRequest.stageId))
      throw StageInfoErrorException("Stage[%s]--UnionStage inputTable number must be more than two".format(sqlStageRequest.stageId))
    }
    var resultDF:DataFrame = null
    for(i<-0 until sqlStageRequest.inputTables.size()){
      val df = appContext.getDataFrame(appContext.getTable(sqlStageRequest.inputTables.get(i)))
      resultDF = if(i==0){
        df
      }else{
        resultDF.unionAll(df)
      }
    }
    resultDF
  }

  override protected def prepare(sqlStageRequest: SqlStageRequest):Boolean={
    if(sqlStageRequest.outPutTable.body==null||sqlStageRequest.outPutTable.body.fields==null){
      sqlStageRequest.outPutTable.body = appContext.getTable(sqlStageRequest.inputTables.get(0)).body
    }
    super.prepare(sqlStageRequest)
  }
}
