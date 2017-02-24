package com.datahandle

import java.util
import java.util.StringTokenizer


import com.boc.iff.CommonFieldConvertorContext
import com.boc.iff.exception.StageInfoErrorException
import com.boc.iff.model.IFFField
import com.context.{SqlStageRequest, StageRequest}
import com.datahandle.tran. FunctionExecutor
import com.model.TableInfo
import org.apache.spark.sql.DataFrame

import scala.collection.mutable.ArrayBuffer
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DataTypes, StructField}

/**
  * Created by scutlxj on 2017/2/22.
  */
class TransformerStageHandle[T<:StageRequest] extends SqlStageHandle[T]{

  protected val functions = Array("TO_CHAR","TO_DATE","SUBSTRING","LENGTH","TO_UPPERCASE","TO_LOWERCASE")
  protected val functionObjName = "fn"

  override protected def handle(sqlStageRequest: SqlStageRequest):DataFrame={
    if(sqlStageRequest.inputTables.size()>1){
      logBuilder.error("Stage[%s]--TransformerStage can only set one inputTable ".format(sqlStageRequest.stageId))
      throw StageInfoErrorException("Stage[%s]--TransformerStage can only set one inputTable ".format(sqlStageRequest.stageId))
    }

    val inputTableInfo = appContext.getTable(sqlStageRequest.inputTables.get(0))
    val inputDF = appContext.getDataFrame(inputTableInfo)
    val outputTable = sqlStageRequest.outPutTable
    for(field <- outputTable.body.fields){
      field.expression = processMethod(field.fieldExpression)
      field.initExpression
    }
    val inputField = inputTableInfo.body.fields.filter(!_.filter)
    val fun = new FunctionExecutor
    val mapFun = (r:Row)=>{
      import com.boc.iff.CommonFieldConvertorContext._
      implicit val fieldConvertorContext = new CommonFieldConvertorContext(null,null,null)
      val newData = new ArrayBuffer[String]
      val valueObjectMap =  new util.HashMap[String,Any]
      //把输入的一列装载到hashMap
      for(index<-0 until inputField.length){
        val fieldValue = r.get(index).asInstanceOf[String]
        valueObjectMap.put(inputField(index).name.toUpperCase(),inputField(index).toObject(fieldValue))
      }
      for(field <- outputTable.body.fields){
        valueObjectMap.put("fn",fun)
        newData+=field.objectToString(field.getValue(valueObjectMap))
      }
      Row.fromSeq(newData)
    }
    val newRdd = inputDF.map(mapFun)
    val structFields = new util.ArrayList[StructField]()
    for (f <- sqlStageRequest.outPutTable.body.fields) {
      structFields.add(DataTypes.createStructField(f.name, DataTypes.StringType, true))
    }
    val structType = DataTypes.createStructType(structFields)
    appContext.sqlContext.createDataFrame(newRdd,structType)
  }

  protected def processMethod(express:String):String={
    println("**********************expression1:"+express)
    var exp = express
    for(f<-functions){
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
          rplExp.append(functionObjName).append(".").append(f.toLowerCase())
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
    println("**********************expression:"+exp)
    exp
  }
}
class TransformerInfo(val stageId:String) extends Serializable{
  var filed:IFFField = _
  var method:String = ""
  var params:ArrayBuffer[String] = new ArrayBuffer[String]
  var dataIndex:Int = _
  var needTran:Boolean = false
  def buildInfo(outputField:IFFField,inputTable:TableInfo):TransformerInfo={
    val exp = outputField.fieldExpression
    var fieldName:String = ""
    if(exp.indexOf("(")>=0) {
      needTran = true
      val stringTokenizer = new StringTokenizer(exp, "(,)")
      method = stringTokenizer.nextToken().toLowerCase
      fieldName = stringTokenizer.nextToken()
      if(stringTokenizer.hasMoreTokens){
        params+=stringTokenizer.nextToken()
      }
    }else{
      fieldName = exp
    }
    filed = inputTable.body.getFieldByName(fieldName)
    if(filed==null){
      throw StageInfoErrorException("Stage[%s]--%s not exists ".format(stageId,fieldName))
    }
    dataIndex = inputTable.body.fields.filter(!_.filter).indexOf(filed)
    if(dataIndex<0){
      throw StageInfoErrorException("Stage[%s]--%s not exists ".format(stageId,fieldName))
    }
    this
  }



}
