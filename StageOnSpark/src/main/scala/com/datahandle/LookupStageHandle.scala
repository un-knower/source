package com.datahandle
import java.util

import com.boc.iff.model.{CDate, CDecimal, CInteger}
import com.context.{SqlStageRequest, StageRequest}
import com.model.{DeduplicateInfo, LookupStageInfo}
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.types.{DataTypes, StructField}
import org.apache.spark.sql.{DataFrame, Row}
/**
  * Created by scutlxj on 2017/4/10.
  */
class LookupStageHandle[T<:StageRequest] extends SqlStageHandle[T]{
  override protected def handle(sqlStageRequest: SqlStageRequest): DataFrame = {
    val lookupStageInfo = sqlStageRequest.asInstanceOf[LookupStageInfo]
    for(i<-0 until lookupStageInfo.deduplicateInfos.size()) {
      val d = lookupStageInfo.deduplicateInfos.get(i)
      val tableInfo = appContext.getTable(d.tableName)
      val df = appContext.getDataFrame(tableInfo)
      val fields = tableInfo.body.fields.filter(!_.filter)
      val pkIndex= for (ki <- 0 until d.keys.size()) yield tableInfo.body.getFieldIndex(d.keys.get(ki))
      val mapPk = (row: Row) => {
        val key = pkIndex.map(x => row(x).toString).reduceLeft(_ + _)
        (key, row)
      }
      val comparator = getComparator(d)
      val rdd = df.rdd.map(mapPk).reduceByKey((x, y) => comparator(x, y)).map(_._2)
      val structFields = new util.ArrayList[StructField]()
      for (f <- fields) {
        val tp = (f.typeInfo match {
          case fieldType: CInteger => DataTypes.LongType
          case fieldType: CDecimal => DataTypes.DoubleType
          case fieldType: CDate => DataTypes.DateType
          case _ => DataTypes.StringType
        })
        structFields.add(DataTypes.createStructField(f.name.toUpperCase, tp, true))
      }
      val structType = DataTypes.createStructType(structFields)
      val dataFrame = appContext.sqlContext.createDataFrame(rdd,structType)
      dataFrame.registerTempTable(d.tableName+"_@"+lookupStageInfo.stageId)
    }
    null
  }

  private def getComparator(deduplicateInfo: DeduplicateInfo):(Row,Row)=>Row={
    val table = appContext.getTable(deduplicateInfo.tableName)
    val fields = table.body.fields
    val sorts = for(i<-0 until deduplicateInfo.sorts.size())yield {
      val sorts = StringUtils.split(deduplicateInfo.sorts.get(i)," ")
      val index = fields.indexOf(table.body.getFieldByName(sorts(0)))
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
            }else{
              o2.toString.compareTo(o1.toString)
            }
          }
          case _ => if (o1 == null)  -1 else if (o2 == null) 1 else{
            if(o1.isInstanceOf[Long]){
              o1.asInstanceOf[Long].compareTo(o2.asInstanceOf[Long])
            }else if(o1.isInstanceOf[java.lang.Double]){
              o1.asInstanceOf[java.lang.Double].compareTo(o2.asInstanceOf[java.lang.Double])
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
