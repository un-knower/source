package com.datahandle
import com.boc.iff.exception.StageInfoErrorException
import com.context.{SqlStageRequest, StageRequest}
import com.model.TableInfo
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.DataFrame

import scala.collection.mutable.HashMap
import scala.collection.mutable.ArrayBuffer

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
    val inputInfoMap = analysisInput(sqlStageRequest.outputTable)
    var i:Int = 0
    val keys = inputInfoMap.keys.iterator
    while(keys.hasNext){
      val key = keys.next()
      resultDF = if(i==0){
        getDataFrame(key,inputInfoMap(key).toArray)
      }else{
        resultDF.unionAll(getDataFrame(key,inputInfoMap(key).toArray))
      }
      i+=1
    }
    try {
      resultDF.first()
    }catch{
      case t:Throwable =>
    }
    resultDF
  }

  protected def analysisInput(outputTable:TableInfo):HashMap[String,ArrayBuffer[String]]={
    val map = new HashMap[String,ArrayBuffer[String]]
    for(field<-outputTable.body.fields){
      val cols = StringUtils.split(field.fieldExpression,",")
      for(col<-cols){
        val tableCol = StringUtils.split(col,".")
        if(!map.contains(tableCol(0))){
          map+=(tableCol(0) -> new ArrayBuffer[String])
        }
        val ab = map(tableCol(0))
        ab+=tableCol(1)+" as " +field.name
      }
    }
    map
  }

  protected def getDataFrame(table:String,cols:Array[String]):DataFrame={
    val df = appContext.getDataFrame(table)
    df.selectExpr(cols:_*)
  }


  override protected def prepare(sqlStageRequest: SqlStageRequest):Boolean={
    if(sqlStageRequest.outputTable.body==null||sqlStageRequest.outputTable.body.fields==null){
      sqlStageRequest.outputTable.body = appContext.getTable(sqlStageRequest.inputTables.get(0)).body
    }
    super.prepare(sqlStageRequest)
  }

  override protected def fillOutPutTable(sqlStageRequest: SqlStageRequest):Unit = {
    for(f<-sqlStageRequest.outputTable.body.fields){
      if(f.typeInfo==null){
        val cols =StringUtils.split(StringUtils.split(f.fieldExpression,",")(0),".")
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
