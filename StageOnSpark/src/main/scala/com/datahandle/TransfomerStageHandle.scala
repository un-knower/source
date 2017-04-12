package com.datahandle

import java.io.FileInputStream
import java.util
import java.util.Properties

import com.boc.iff.{CommonFieldConvertorContext, ECCLogger}
import com.boc.iff.exception.{MaxErrorNumberException, StageInfoErrorException}
import com.boc.iff.model.{CDate, CDecimal, CInteger, CTime, IFFField}
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
      field.expression = processMethod(field.fieldExpression,field)
      field.initExpression
    }
    val inputField = inputTableInfo.body.fields.filter(!_.filter)
    val fun = new FunctionExecutor

    import com.boc.iff.CommonFieldConvertorContext._
    implicit val fieldConvertorContext = new CommonFieldConvertorContext(null, null, null)
    val mapFun:(Row => Row)= { r =>
      val newData = new ArrayBuffer[Any]
      val valueObjectMap = new util.HashMap[String, Any]
      for (index <- 0 until inputField.length) {
        valueObjectMap.put(inputField(index).name.toUpperCase(), r.get(index))
      }
      var express = ""
      valueObjectMap.put("fn", fun)
      for (field <- outputTable.body.fields) {
        express = field.fieldExpression
        val value = if (valueObjectMap.containsKey(express)) valueObjectMap.get(express) else field.getValue(valueObjectMap)
        val newValue = field.typeInfo match {
          case fieldType: CTime => field.objectToString(value)
          case _ => value
        }
        newData += newValue
      }
      Row.fromSeq(newData)
    }

    /*val pro = new Properties
    pro.load(new FileInputStream(appContext.jobConfig.configPath))
    val mapPartitionFun:(Iterator[Row] => Iterator[Row])= { rs =>
      val logger = new ECCLogger
      logger.configure(pro)
      logger.info("TransformerStageHandle","***********************************TransformerStageHandle************************************8")
      val resultList = new ListBuffer[Row]
      while(rs.hasNext) {
        val r = rs.next()
        resultList += mapFun(r)
      }
      resultList.iterator
    }*/



    val newRdd = inputDF.map(mapFun)
    val structFields = new util.ArrayList[StructField]()
    for (f <- sqlStageRequest.outputTable.body.fields) {
      val tp = (f.typeInfo match {
        case fieldType: CInteger => DataTypes.LongType
        case fieldType: CDecimal => DataTypes.DoubleType
        case fieldType: CDate => DataTypes.DateType
        case _ => DataTypes.StringType
      })
      structFields.add(DataTypes.createStructField(f.name.toUpperCase, tp, true))
    }
    val structType = DataTypes.createStructType(structFields)
    val df = appContext.sqlContext.createDataFrame(newRdd,structType)
    df
  }

  protected def processMethod(express:String,field:IFFField):String={
    var exp = express
    for(func<-functions){
      val f = "FN."+func
      val replaceFun = "fn." + (if(func.equals("TO_NUMBER") && field.typeInfo.isInstanceOf[CDecimal])"to_double" else func.toLowerCase())
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
          rplExp.append(replaceFun)
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
