package com.boc.iff.history

import com.boc.iff.IFFConversion._
import com.boc.iff.IFFUtils
import com.boc.iff.exception.PrimaryKeyMissException
import com.boc.iff.itf.DataProcessOnSparkConfig
import com.boc.iff.model.IFFMetadata
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.DataFrame

/**
  * Created by scutlxj on 2016/12/8.
  */
class I2FHistoryOnSparkJob  extends HistoryProcessOnSparkJob with Serializable {

  override def diffHistoryAndIncrease(increase:DataFrame, history:DataFrame, iffMetadata: IFFMetadata):(DataFrame,DataFrame)={
    val primaryKeys = iffMetadata.body.fields.filter(_.primaryKey)
    if(primaryKeys==null||primaryKeys.size==0){
      throw PrimaryKeyMissException("Primary Key of table"+dataProcessConfig.fTableName+" is required")
    }
    val fields = iffMetadata.body.fields.filter(!_.filter)
    val acDate = IFFUtils.dateToString(dataProcessConfig.accountDate)
    val lastAcDate = IFFUtils.addDays(acDate,-1)
    val newOpenSQL = new StringBuffer("select ")
    var index:Int = 0
    for(f<-fields){
      if(index>0){
        newOpenSQL.append(" , ")
      }
      newOpenSQL.append("i."+f.name)
      index+=1
    }
    newOpenSQL.append(" ,'"+acDate+"' as "+this.beginDTName+",'99991231' as "+this.endDTName+" from incTB i ")
    increase.registerTempTable("incTB")
    var closeDF:DataFrame = null
    var newOpen = sqlContext.sql(newOpenSQL.toString)
    if(history!=null) {
      val hisSQL = new StringBuffer("select ")
      index = 0
      for(f<-fields){
        if(index>0){
          hisSQL.append(" , ")
        }
        hisSQL.append("f."+f.name)
        index+=1
      }
      hisSQL.append(" from hisTB f left join incTB i on ")
      index=0
      for(p<-primaryKeys){
        if(index>0){
          hisSQL.append(" and ")
        }
        hisSQL.append("f."+p.name+"=i."+p.name)
      }
      history.registerTempTable("hisTB")
      val hisDF = sqlContext.sql(hisSQL.toString)
      hisDF.cache()
      closeDF = hisDF.filter("i."+primaryKeys(0).name+" is not null ").selectExpr("*","f."+this.beginDTName,"'"+lastAcDate+"' as "+this.endDTName)
      val stillOpenDF = hisDF.filter("i."+primaryKeys(0).name+" is null ").selectExpr("*","f."+this.beginDTName,"f."+this.endDTName)
      newOpen = newOpen.unionAll(stillOpenDF)
      hisDF.unpersist()
    }
    (closeDF,newOpen)
  }

}

/**
  * Spark 程序入口
  */
object I2FHistoryOnSpark extends App {
  val config = new DataProcessOnSparkConfig()
  val job = new I2FHistoryOnSparkJob() with ParquetDateReader with ParquetDataWriter
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
