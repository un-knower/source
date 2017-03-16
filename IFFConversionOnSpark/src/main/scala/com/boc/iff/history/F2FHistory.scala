package com.boc.iff.history

import com.boc.iff.{DFSUtils, IFFUtils}
import com.boc.iff.IFFConversion._
import com.boc.iff.itf.DataProcessOnSparkConfig
import com.boc.iff.model._
import com.boc.iff.exception._
import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.DataFrame

import scala.collection.mutable.ArrayBuffer

class F2FHistoryOnSparkJob  extends HistoryProcessOnSparkJob with Serializable {
  /**
   *  拉链 历史 加工
  **/
  override def diffHistoryAndIncrease(increase:DataFrame,history:DataFrame,iffMetadata: IFFMetadata):(DataFrame,DataFrame)={
    increase.registerTempTable("inc")
    val primaryKeys = iffMetadata.getBody.fields.filter(_.primaryKey)
    val fields = iffMetadata.getBody.fields.filter(!_.filter)
    val diffFields = iffMetadata.getBody.fields.filter(_.diffField)
    val acDate = IFFUtils.dateToString(dataProcessConfig.accountDate)
    val newOpen = new StringBuffer(" select ")//新打开的查询语句
    if(history==null){//单表增量转拉链历史      val newOpen = new StringBuffer(" select ")
      var index = 0
      for(f<-fields){
        if(index>0){
          newOpen.append(" , ")
        }
        newOpen.append(" i."+f.getName)
        index+=1
      }
      newOpen.append(",'"+acDate+"' as "+this.beginDTName+", '9999-12-30' as "+this.endDTName+" from inc i ")
      (null,sqlContext.sql(newOpen.toString))
    }else{
      if(primaryKeys==null||primaryKeys.size==0){
        throw PrimaryKeyMissException("Primary Key of table"+dataProcessConfig.fTableName+" is required")
      }
      val lastAcDate = IFFUtils.addDays(acDate,-1)
      history.registerTempTable("hist")
      val hisSql = new StringBuffer(" select ")//历史的查询语句
      val condition = new StringBuffer(" ")//关联条件
      val diffCondition = new StringBuffer(" ")
      var index = 0
      for(f<-fields){
        if(index>0){
          hisSql.append(" , ")
          newOpen.append(" , ")
        }
        hisSql.append(" h."+f.getName+" as h"+f.getName)
        hisSql.append(" , ")
        hisSql.append(" i."+f.getName+" as i"+f.getName)
        newOpen.append(" i."+f.getName)
        index+=1
      }
      index = 0
      for(f<-primaryKeys){
        if(index>0){
          condition.append(" and ")
        }
        condition.append(" h."+f.getName+" = i."+f.getName)
        index+=1
      }
      index = 0
      for(f<-diffFields){
        if(index>0){
          diffCondition.append(" or ")
        }
        diffCondition.append(" h"+f.getName+" != i"+f.getName)
        index+=1
      }
      if(index==0)//no diff fields
        diffCondition.append(" 1=1 ")
      hisSql.append(",h."+this.beginDTName+" as fBeginDT,h."+this.endDTName+" as fEndDT,i."+primaryKeys(0).name+" as incKey from hist h left join inc i on ")
      hisSql.append(condition)
      val hisDF = sqlContext.sql(hisSql.toString)
      hisDF.cache()
      var closeDF = hisDF.filter("incKey is not null  and (("+diffCondition+"))").selectExpr("*","fBeginDT as "+this.beginDTName,"'"+lastAcDate+"' as "+this.endDTName).drop("incKey").drop("fBeginDT").drop("fEndDT")
      for(f<-fields){
        closeDF = closeDF.drop("i"+f.getName)
      }
      var stillOpenDF1 = hisDF.filter("incKey is not null").selectExpr("*","fBeginDT as "+this.beginDTName,"fEndDT as "+this.endDTName).drop("incKey").drop("fBeginDT").drop("fEndDT")
      for(f<-fields){
        stillOpenDF1 = stillOpenDF1.drop("h"+f.getName)
      }
      var stillOpenDF2 = hisDF.filter("incKey is null ").selectExpr("*","fBeginDT as "+this.beginDTName,"fEndDT as "+this.endDTName).drop("incKey").drop("fBeginDT").drop("fEndDT")
      for(f<-fields){
        stillOpenDF2 = stillOpenDF2.drop("i"+f.getName)
      }
      hisDF.unpersist()
      newOpen.append(",'"+acDate+"' as "+this.beginDTName+", '9999-12-30' as "+this.endDTName+" from inc i left join hist h on ")
      newOpen.append(condition)
      newOpen.append(" where h."+primaryKeys(0).getName +" is null")
      val newOpenDF = sqlContext.sql(newOpen.toString)
      (closeDF,(stillOpenDF1.unionAll(stillOpenDF2)).unionAll(newOpenDF))
    }
  }
}

/**
 * Spark 程序入口
 */
object F2FHistoryOnSpark extends App {
  val config = new DataProcessOnSparkConfig()
  val job = new F2FHistoryOnSparkJob() with TextDateReader with TextDataWriter
  val logger = job.logger
  try {
    job.start(config, args)
  } catch {
    case t: Throwable =>
      t.printStackTrace()
      if (StringUtils.isNotEmpty(t.getMessage)) logger.error(MESSAGE_ID_CNV1001, t.getMessage)
      System.exit(1)
  }
}
