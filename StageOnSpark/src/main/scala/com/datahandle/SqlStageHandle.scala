package com.datahandle

import com.boc.iff.exception.StageInfoErrorException
import com.context.{SqlStageRequest, StageAppContext, StageRequest}
import com.log.LogBuilder
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.DataFrame

/**
  * Created by cvinc on 2016/6/8.
  */
class SqlStageHandle[T<:StageRequest] extends StageHandle[T] {


  override def execute(stRequest: StageRequest): Unit = {
    val sqlStageRequest = stRequest.asInstanceOf[SqlStageRequest]
    if(sqlStageRequest.inputTables==null||sqlStageRequest.inputTables.size()==0){
      logBuilder.error("Stage[%s] -- inputTable required".format(sqlStageRequest.stageId))
      throw StageInfoErrorException("Stage[%s] -- inputTable required".format(sqlStageRequest.stageId))
    }
    prepare(sqlStageRequest)
    var resultDF = handle(sqlStageRequest)
    //结果集过滤
    if(StringUtils.isNotEmpty(sqlStageRequest.logicFilter)){
      resultDF = resultDF.filter(sqlStageRequest.logicFilter)
    }
    //提取结果集数量
    if(sqlStageRequest.limitFilter>0){
      resultDF = resultDF.limit(sqlStageRequest.limitFilter)
    }
    if(appContext.jobConfig.debug&&sqlStageRequest.debugInfo!=null&&(!"IGNORE".equals(sqlStageRequest.debugInfo.method))){
      if(StringUtils.isEmpty(sqlStageRequest.debugInfo.file)){
        sqlStageRequest.debugInfo.file = "%s/%s/%s".format(appContext.jobConfig.defaultDebugFilePath,appContext.sparkContext.applicationId,sqlStageRequest.stageId)
      }
      saveDebug(sqlStageRequest.debugInfo,resultDF)
    }
    appContext.addDataSet(sqlStageRequest.outputTable,resultDF)
    appContext.addTable(sqlStageRequest.outputTable)
  }

  protected def prepare(sqlStageRequest: SqlStageRequest):Boolean={
    loadFieldTypeInfo(sqlStageRequest.outputTable)
    fillOutPutTable(sqlStageRequest)
    true
  }

  protected def handle(sqlStageRequest: SqlStageRequest):DataFrame={
    val sql = getSql(sqlStageRequest)
    appContext.sqlContext.sql(sql)
  }

  protected def fillOutPutTable(sqlStageRequest: SqlStageRequest):Unit = {
    val sourceTableInfo = appContext.getTable(sqlStageRequest.inputTables.get(0))
    for (f <- sqlStageRequest.outputTable.body.fields) {
      if (f.typeInfo == null) {
        val sourceField = sourceTableInfo.getBody.getFieldByName(f.fieldExpression)
        if (sourceField == null) {
          logBuilder.error("Stage[%s]--Sql type of %s is required ".format(sqlStageRequest.stageId, f.name))
          throw StageInfoErrorException("Stage[%s]--Sql type of %s is required ".format(sqlStageRequest.stageId, f.name))
        }
        f.typeInfo = sourceField.typeInfo
      }
    }
  }


  protected def getSql(sqlStageRequest: SqlStageRequest):String={
    val sql = new StringBuffer(" select ")
    val outPutFields = sqlStageRequest.outputTable.body.fields
    var firstColF = true
    for(field<-outPutFields){
      if(!firstColF){
        sql.append(",")
      }
      sql.append(field.fieldExpression).append(" as ").append(field.name)
      firstColF = false
    }
    if(StringUtils.isEmpty(sqlStageRequest.from)){
      throw new StageInfoErrorException("Stage[%s]-xml define error, property from is required".format(sqlStageRequest.stageId))
    }
    sql.append(" from ").append(sqlStageRequest.from)
    if(StringUtils.isNotEmpty(sqlStageRequest.groupBy)){
      sql.append(" group by ").append(sqlStageRequest.groupBy)
    }
    if(sqlStageRequest.sorts!=null&&sqlStageRequest.sorts.size()>0){
      val sortStr = new StringBuffer()
      for(i<-0 until sqlStageRequest.sorts.size){
        if(i>0)sortStr.append(",")
        sortStr.append(sqlStageRequest.sorts.get(i))
      }
      sql.append(" order by ").append(sortStr)
    }
    logBuilder.info("Stage Sql["+sql.toString+"]")
    replaceArgs(sql.toString)
  }

  protected def replaceArgs(sql:String):String={
    var processSql = sql
    if(appContext.batchArgName!=null&&appContext.batchArgName.length>0){
      for(i<- 0 until appContext.batchArgName.length ){
        val argString = "#"+appContext.batchArgName(i)+"#"
        processSql = StringUtils.replace(processSql,argString,appContext.batchArgs(i))
      }
    }
    processSql
  }



}



