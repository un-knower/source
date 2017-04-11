package com.datahandle

import com.context.{SqlStageRequest, StageRequest}
import com.model.DeduplicateInfo
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.{DataFrame, Row}

/**
  * Created by scutlxj on 2017/4/10.
  */
class MergeStageHandle [T<:StageRequest] extends SqlStageHandle[T]{
  override protected def handle(sqlStageRequest: SqlStageRequest): DataFrame = {
    ???
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
      if(result>0) r2 else r1
    }
    comparator
  }

}
