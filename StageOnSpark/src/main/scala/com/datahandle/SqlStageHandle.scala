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
  var appContext:StageAppContext = _
  var logBuilder:LogBuilder = _

  override def doCommand(stRequest: StageRequest)(implicit appContext:StageAppContext): Unit = {
    this.appContext = appContext
    logBuilder = appContext.constructLogBuilder()
    logBuilder.setLogThreadID(Thread.currentThread().getId.toString)
    val sqlStageRequest = stRequest.asInstanceOf[SqlStageRequest]
    if(sqlStageRequest.inputTables==null||sqlStageRequest.inputTables.size()==0){
      logBuilder.error("Stage[%s] -- inputTable required".format(sqlStageRequest.stageId))
      throw StageInfoErrorException("Stage[%s] -- inputTable required".format(sqlStageRequest.stageId))
    }
    var resultDF = handle(sqlStageRequest)
    //结果集过滤
    if(StringUtils.isNotEmpty(sqlStageRequest.logicFilter)){
      resultDF = resultDF.filter(sqlStageRequest.logicFilter)
    }
    //提取结果集数量
    if(sqlStageRequest.limitFilter>0){
      resultDF = resultDF.limit(sqlStageRequest.limitFilter)
    }
    appContext.addDataSet(sqlStageRequest.outPutTable,resultDF)
    appContext.addTable(sqlStageRequest.outPutTable)
  }

  protected def handle(sqlStageRequest: SqlStageRequest):DataFrame={
    val sql = getSql(sqlStageRequest)
    appContext.sqlContext.sql(sql)
  }


  protected def getSql(sqlStageRequest: SqlStageRequest):String={
    val sql = new StringBuffer(" select ")
    val outPutFields = sqlStageRequest.outPutTable.body.fields
    var firstColF = true
    for(field<-outPutFields){
      if(!firstColF){
        sql.append(",")
      }
      sql.append(field.fieldExpression).append(" as ").append(field.name)
      firstColF = false
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
    sql.toString
  }



}



