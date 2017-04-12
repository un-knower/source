package com.datahandle

import java.util
import java.lang.Long

import com.boc.iff.exception.StageInfoErrorException
import com.boc.iff.model.{CDate, CDecimal, CInteger}
import com.context.{SqlStageRequest, StageRequest}
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{DataTypes, StructField}
import org.apache.spark.sql.{DataFrame, Row, SQLContext}

import scala.collection.mutable.ArrayBuffer

/**
  * Created by scutlxj on 2017/2/14.
  */
class DeduplicateStageHandle[T<:StageRequest] extends SqlStageHandle[T]{

  override protected def handle(sqlStageRequest: SqlStageRequest): DataFrame = {
    if(sqlStageRequest.inputTables.size()>1){
      logBuilder.error("Stage[%s]--DeduplicateStage can only set one inputTable".format(sqlStageRequest.stageId))
      throw StageInfoErrorException("Stage[%s]--DeduplicateStage can only set one inputTable".format(sqlStageRequest.stageId))
    }
    if(StringUtils.isEmpty(sqlStageRequest.from)){
      sqlStageRequest.from = sqlStageRequest.inputTables.get(0)
    }
    if(sqlStageRequest.outputTable.body.fields.filter(_.primaryKey).length==0){
      logBuilder.error("Stage[%s]--DeduplicateStage require key".format(sqlStageRequest.stageId))
      throw StageInfoErrorException("Stage[%s]--DeduplicateStage require key".format(sqlStageRequest.stageId))
    }
    if(sqlStageRequest.sorts==null||sqlStageRequest.sorts.size()==0){
      logBuilder.error("Stage[%s]--DeduplicateStage require sorts".format(sqlStageRequest.stageId))
      throw StageInfoErrorException("Stage[%s]--DeduplicateStage require sorts".format(sqlStageRequest.stageId))
    }

    val df = appContext.getDataFrame(sqlStageRequest.from)
    val fields = sqlStageRequest.outputTable.body.fields
    val pkFields = fields.filter(_.primaryKey)
    val pkIndex = for(f<-pkFields)yield fields.indexOf(f)
    val mapPk = (row:Row)=>{
      val key = pkIndex.map(x=>row(x).toString).reduceLeft(_+_)
      (key,row)
    }
    val comparator = getComparator(sqlStageRequest)
    val rdd = df.rdd.map(mapPk).reduceByKey((x,y)=>comparator(x,y)).map(_._2)
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
    appContext.sqlContext.createDataFrame(rdd,structType)
  }

  private def getComparator(sqlStageRequest: SqlStageRequest):(Row,Row)=>Row={
    val fields = sqlStageRequest.outputTable.body.fields
    val sorts = for(i<-0 until sqlStageRequest.sorts.size())yield {
      val sorts = StringUtils.split(sqlStageRequest.sorts.get(i)," ")
      val index = fields.indexOf(sqlStageRequest.outputTable.body.getFieldByName(sorts(0)))
      (index,sorts(1).toLowerCase)
    }
    println("*******************************"+sorts.mkString)
    val comparator = (r1:Row,r2:Row)=>{
      var notEnd = true
      var result = 0
      for(s<-sorts if(notEnd)){
        val o1 = r1(s._1)
        val o2 = r2(s._1)
        result = s._2 match {
          case "desc" => if (o2 == null)  -1 else if (o1 == null) 1 else {
            if(o1.isInstanceOf[Long]){
              o2.asInstanceOf[Long].compareTo(o1.asInstanceOf[Long])
            }else if(o1.isInstanceOf[java.lang.Double]){
              o2.asInstanceOf[java.lang.Double].compareTo(o1.asInstanceOf[java.lang.Double])
            }else if(o1.isInstanceOf[java.util.Date]){
              o2.asInstanceOf[java.util.Date].compareTo(o1.asInstanceOf[java.util.Date])
            }else{
              o2.toString.compareTo(o1.toString)
            }
          }
          case _ => if (o1 == null)  -1 else if (o2 == null) 1 else{
            if(o1.isInstanceOf[Long]){
              o1.asInstanceOf[Long].compareTo(o2.asInstanceOf[Long])
            }else if(o1.isInstanceOf[java.lang.Double]){
              o1.asInstanceOf[java.lang.Double].compareTo(o2.asInstanceOf[java.lang.Double])
            }else if(o1.isInstanceOf[java.util.Date]){
              o1.asInstanceOf[java.util.Date].compareTo(o2.asInstanceOf[java.util.Date])
            }else{
              o1.toString.compareTo(o2.toString)
            }
          }
        }
        if(result!=0)notEnd=false
      }
      if(result>0) r2 else r1
    }
    comparator
  }


}
