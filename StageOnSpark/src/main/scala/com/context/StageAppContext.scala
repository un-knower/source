package com.context

import java.util.concurrent.ConcurrentHashMap
import java.util.HashMap

import com.config.SparkJobConfig
import com.model.{StageInfo, TableInfo}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext}


/**
  * Created by cvinc on 2016/6/8.
  */
class StageAppContext(val sparkContext:SparkContext,val jobConfig:SparkJobConfig)  {

  val sqlContext:SQLContext = new SQLContext(sparkContext)

  val tablesMap:ConcurrentHashMap[String,TableInfo] = new ConcurrentHashMap[String,TableInfo]

  private val dataSetObjectMap:ConcurrentHashMap[String,DataFrame] = new ConcurrentHashMap[String,DataFrame]

  val stagesMap:HashMap[String,StageInfo] = new HashMap[String,StageInfo]

  var fistStage:StageInfo = _


  def getDataFrame(tableInfo:TableInfo):DataFrame={
    if(dataSetObjectMap.containsKey(tableInfo.targetName)){
      dataSetObjectMap.get(tableInfo.targetName)
    }else{
      null
    }
  }


  def addDataSet(tableInfo:TableInfo,dataSet:DataFrame): Unit ={
    //println("******Add DateSet:"+tableInfo.targetName+" rowCount:"+dataSet.count())
    dataSetObjectMap.put(tableInfo.targetName,dataSet)
  }


}
