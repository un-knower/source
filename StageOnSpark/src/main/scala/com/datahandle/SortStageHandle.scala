package com.datahandle

import java.io.FileInputStream
import java.util
import java.util.Properties

import com.boc.iff.ECCLogger
import com.boc.iff.exception.StageInfoErrorException
import com.boc.iff.model.{CDate, CDecimal, CInteger}
import com.context.{SqlStageRequest, StageRequest}
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.types.{DataTypes, StructField}
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable.{ArrayBuffer, ListBuffer}

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
    val fields = sqlStageRequest.outputTable.getBody.fields
    val randIndex = sqlStageRequest.outputTable.getBody.getFieldIndex("_RAND_ID")
    val rowIndex = sqlStageRequest.outputTable.getBody.getFieldIndex("_ROW_ID")
    if(randIndex>=0||rowIndex>=0){
      val comparator = getComparator(sqlStageRequest)
      val checkEqual = getCheckEqual(sqlStageRequest)
      val sortPartition = (rows:Iterator[Row])=>{
        val rowSorted = rows.toList.sortWith(comparator)
        val recordList = ListBuffer[Row]()
        var rowId:Integer= 0
        var randId:Integer = 0
        var lastRow:Row = null
        var index = 0
        var arrayBuffer:ArrayBuffer[Any] = null
        for(r<-rowSorted){
          index = 0
          arrayBuffer =  new ArrayBuffer[Any]()
          for(f<-fields){
            if(f.name.toUpperCase().equals("_RAND_ID")){
              if(lastRow==null||(!checkEqual(lastRow,r))) {
                randId += 1
              }
              arrayBuffer+=randId
              lastRow = r
            }else if(f.name.toUpperCase().equals("_ROW_ID")){
              rowId += 1
              arrayBuffer+=rowId
            }else{
              arrayBuffer+=r(index)
              index += 1
            }
          }
          recordList += Row.fromSeq(arrayBuffer)
        }
        recordList.iterator
      }
      val rdd = df.rdd
      val numberPartition = rdd.getNumPartitions
      val newRdd = rdd.repartition(1).mapPartitions(sortPartition)
      val structFields = new util.ArrayList[StructField]()
      for(f <- fields) {
        val tp = (f.typeInfo match {
          case fieldType: CInteger => DataTypes.LongType
          case fieldType: CDecimal => DataTypes.DoubleType
          case fieldType: CDate => DataTypes.DateType
          case _ => DataTypes.StringType
        })
        structFields.add(DataTypes.createStructField(f.name.toUpperCase, tp, true))
      }
      val structType = DataTypes.createStructType(structFields)
      appContext.sqlContext.createDataFrame(newRdd.repartition(numberPartition),structType)
    }else{
      df
    }
  }

  private def getCheckEqual(sqlStageRequest: SqlStageRequest):(Row,Row)=>Boolean={
    val fields = sqlStageRequest.outputTable.body.fields.filter(x=>(!(x.name.toUpperCase().endsWith("_RAND_ID")||x.name.toUpperCase().endsWith("_ROW_ID"))))
    val sorts = for(i<-0 until sqlStageRequest.sorts.size())yield {
      val sorts = StringUtils.split(sqlStageRequest.sorts.get(i)," ")
      val index = fields.indexOf(sqlStageRequest.outputTable.body.getFieldByName(sorts(0)))
      (index,sorts(1).toLowerCase)
    }
    val checkEqual = (r1:Row,r2:Row)=>{
      var result = true
      for(s<-sorts if(result)) {
        val o1 = r1(s._1)
        val o2 = r2(s._1)
        result = if (o2 == null && o1 == null) true else if (o2 == null && o1 != null || o1 == null && o2 != null) false else o2.toString.compareTo(o1.toString) == 0
      }
      result
    }
    checkEqual
  }

  private def getComparator(sqlStageRequest: SqlStageRequest):(Row,Row)=>Boolean={
    val fields = sqlStageRequest.outputTable.body.fields.filter(x=>(!(x.name.toUpperCase().endsWith("_RAND_ID")||x.name.toUpperCase().endsWith("_ROW_ID"))))
    val sorts = for(i<-0 until sqlStageRequest.sorts.size())yield {
      val sorts = StringUtils.split(sqlStageRequest.sorts.get(i)," ")
      val index = fields.indexOf(sqlStageRequest.outputTable.body.getFieldByName(sorts(0)))
      (index,sorts(1).toLowerCase)
    }
    val comparator = (r1:Row,r2:Row)=>{
      var notEnd = true
      var result = 0
      for(s<-sorts if(notEnd)){
        val o1 = r1(s._1)
        val o2 = r2(s._1)
        result = s._2 match {
          case "desc" => if (o2 == null)  -1 else if (o1 == null) 1 else {
            if(o1.isInstanceOf[Integer]){
              o2.asInstanceOf[Integer].compareTo(o1.asInstanceOf[Integer])
            }else if(o1.isInstanceOf[java.lang.Double]){
              o2.asInstanceOf[java.lang.Double].compareTo(o1.asInstanceOf[java.lang.Double])
            }else{
              o2.toString.compareTo(o1.toString)
            }
          }
          case _ => if (o1 == null)  -1 else if (o2 == null) 1 else{
            if(o1.isInstanceOf[Integer]){
              o1.asInstanceOf[Integer].compareTo(o2.asInstanceOf[Integer])
            }else if(o1.isInstanceOf[java.lang.Double]){
              o1.asInstanceOf[java.lang.Double].compareTo(o2.asInstanceOf[java.lang.Double])
            }else{
              o1.toString.compareTo(o2.toString)
            }
          }
        }
        if(result!=0)notEnd=false
      }
      if(result<0)true else false
    }
    comparator
  }

  override def filterDF(sqlStageRequest: SqlStageRequest,df:DataFrame) = df

}
