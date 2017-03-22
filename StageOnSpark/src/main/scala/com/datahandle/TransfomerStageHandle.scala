package com.datahandle

import java.io.FileInputStream
import java.util
import java.util.Properties

import com.boc.iff.{CommonFieldConvertorContext, ECCLogger}
import com.boc.iff.exception.{MaxErrorNumberException, StageInfoErrorException}
import com.context.{SqlStageRequest, StageRequest}
import com.datahandle.tran.FunctionExecutor
import com.log.LogBuilder
import org.apache.spark.sql.DataFrame

import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DataTypes, StructField}

/**
  * Created by scutlxj on 2017/2/22.
  */
class TransformerStageHandle[T<:StageRequest] extends SqlStageHandle[T]{

  protected val functions = Array("TO_CHAR","TO_DATE","SUBSTRING","LENGTH","TO_UPPERCASE","TO_LOWERCASE","TO_NUMBER","TRIM","LTRIM","RTRIM","REPLACE","CURRENT_DATE","CURRENT_MONTHEND","TRUNC","ROUND")

  override protected def handle(sqlStageRequest: SqlStageRequest):DataFrame={
    if(sqlStageRequest.inputTables.size()>1){
      logBuilder.error("Stage[%s]--TransformerStage can only set one inputTable ".format(sqlStageRequest.stageId))
      throw StageInfoErrorException("Stage[%s]--TransformerStage can only set one inputTable ".format(sqlStageRequest.stageId))
    }
    val inputTableInfo = appContext.getTable(sqlStageRequest.inputTables.get(0))
    val inputDF = appContext.getDataFrame(inputTableInfo)
    val outputTable = sqlStageRequest.outputTable
    for(field <- outputTable.body.fields){
      field.expression = processMethod(field.fieldExpression)
      field.initExpression
    }
    val inputField = inputTableInfo.body.fields.filter(!_.filter)
    val fun = new FunctionExecutor
    /*
val stageId = sqlStageRequest.stageId
//val errorRcNumber = appContext.sparkContext.accumulator(0, "%s_TransformerErrorRec".format(stageId))
val prop = new Properties
prop.load(new FileInputStream(appContext.jobConfig.configPath))
val applicationId = appContext.sparkContext.applicationId
val maxErrorNumber = 0

val mapFun:(Iterator[Row] => Iterator[Row])= { rows=>
  import com.boc.iff.CommonFieldConvertorContext._
  implicit val fieldConvertorContext = new CommonFieldConvertorContext(null, null, null)

  val logger = new ECCLogger()
  logger.configure(prop)
  val logBuilder = new LogBuilder(logger)
  logBuilder.setLogJobID(applicationId)
  logBuilder.setLogThreadID(Thread.currentThread().getId.toString)

  val recordList = ListBuffer[Row]()
  while(rows.hasNext){
    val r = rows.next()
    val newData = new ArrayBuffer[String]
    val valueObjectMap = new util.HashMap[String, Any]
    for (index <- 0 until inputField.length) {
      val fieldValue = if (r.get(index) != null) r.get(index).toString else ""
      valueObjectMap.put(inputField(index).name.toUpperCase(), inputField(index).toObject(fieldValue))
    }
    var express = ""
    valueObjectMap.put("fn", fun)
    try {
      for (field <- outputTable.body.fields) {
        express = field.fieldExpression
        val value = if(valueObjectMap.containsKey(express))valueObjectMap.get(express) else field.getValue(valueObjectMap)
        newData += field.objectToString(value)
      }
      recordList += Row.fromSeq(newData)
    }catch {
      case t:Throwable=>
        val data = new StringBuffer()
        for (index <- 0 until inputField.length) {
          val fieldValue = if (r.get(index) != null) r.get(index).toString else ""
          data.append(inputField(index).name.toUpperCase()).append("=").append(fieldValue).append("|")
        }
        logBuilder.error("Stage[%s]-{Expression[%s],Data[%s]} Error Msg{%s}".format(stageId,express,data.toString,t.getMessage))
        if(maxErrorNumber==0){
          throw t
        }else {
          //errorRcNumber += 1
        }
    }
  }
  recordList.iterator
}*/
    import com.boc.iff.CommonFieldConvertorContext._
    implicit val fieldConvertorContext = new CommonFieldConvertorContext(null, null, null)
    val mapFun:(Row => Row)= { r =>
      val newData = new ArrayBuffer[String]
      val valueObjectMap = new util.HashMap[String, Any]
      for (index <- 0 until inputField.length) {
        val fieldValue = if (r.get(index) != null) r.get(index).toString else ""
        valueObjectMap.put(inputField(index).name.toUpperCase(), inputField(index).toObject(fieldValue))
      }
      var express = ""
      valueObjectMap.put("fn", fun)
      for (field <- outputTable.body.fields) {
        express = field.fieldExpression
        val value = if (valueObjectMap.containsKey(express)) valueObjectMap.get(express) else field.getValue(valueObjectMap)
        newData += field.objectToString(value)
      }
      Row.fromSeq(newData)
    }
    val newRdd = inputDF.map(mapFun)
    val structFields = new util.ArrayList[StructField]()
    for (f <- sqlStageRequest.outputTable.body.fields) {
      structFields.add(DataTypes.createStructField(f.name, DataTypes.StringType, true))
    }
    val structType = DataTypes.createStructType(structFields)
    val df = appContext.sqlContext.createDataFrame(newRdd,structType)
    /*if(errorRcNumber.value > maxErrorNumber){
      throw new MaxErrorNumberException("Stage[%s]-Transformer Max Error Rec.Limit[%s],actually[%s]".format(sqlStageRequest.stageId,maxErrorNumber,errorRcNumber))
    }*/
    df
  }

  protected def processMethod(express:String):String={
    var exp = express
    for(func<-functions){
      val f = "FN."+func
      val e = exp.toUpperCase()
      var index = 0
      val ar = new ArrayBuffer[(Int, Int)]
      while(e.indexOf(f,index)>=0){
        val i = e.indexOf(f,index)
        ar += ((i,f.length))
        index = i+1
      }
      if(ar.length>0){
        val rplExp = new StringBuffer()
        var i:Int = 0
        while(i<ar.size){
          val pos = ar(i)
          if(i==0&&pos._1>0){
            rplExp.append(exp.substring(0,pos._1))
          }
          rplExp.append(f.toLowerCase())
          val curStr = if((i+1)<ar.size){
            exp.substring(pos._1+pos._2,ar(i+1)._1)
          }else{
            exp.substring(pos._1+pos._2)
          }
          rplExp.append(curStr)
          i+=1
        }
        exp = rplExp.toString
      }
    }
    exp
  }




}
