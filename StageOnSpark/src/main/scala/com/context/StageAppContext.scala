package com.context

import java.util.concurrent.ConcurrentHashMap
import java.util.HashMap

import com.boc.iff.exception.TableLoadException
import com.config.SparkJobConfig
import com.model.{StageInfo, TableInfo}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext}


/**
  * Created by cvinc on 2016/6/8.
  */
class StageAppContext(val sparkContext:SparkContext,val jobConfig:SparkJobConfig)  {

  var currentStage:StageInfo = _

  val sqlContext:SQLContext = new SQLContext(sparkContext)

  private val tablesMap:ConcurrentHashMap[String,TableInfo] = new ConcurrentHashMap[String,TableInfo]

  private val dataSetObjectMap:ConcurrentHashMap[String,DataFrame] = new ConcurrentHashMap[String,DataFrame]

  val stagesMap:HashMap[String,StageInfo] = new HashMap[String,StageInfo]

  var fistStage:StageInfo = _

  def checkTableExist(table:String):Boolean={
    if(tablesMap.containsKey(table)){
      true
    }else{
      false
    }
  }

  def getTable(table:String):TableInfo={
    if(tablesMap.containsKey(table)){
      tablesMap.get(table)
    }else{
      throw TableLoadException("Stage[%s] | Table[%s] can not be found，check if it loaded".format(currentStage.stageId,table))
    }
  }

  def addTable(table:TableInfo):TableInfo={
    tablesMap.put(table.targetName,table)
  }


  def getDataFrame(tableInfo:TableInfo):DataFrame={
    if(dataSetObjectMap.containsKey(tableInfo.targetName)){
      dataSetObjectMap.get(tableInfo.targetName)
    }else{
      throw TableLoadException("Stage[%s] | Table[%s] can not be found，check if it loaded".format(currentStage.stageId,tableInfo.targetName))
    }
  }

  def getDataFrame(table:String):DataFrame={
    if(dataSetObjectMap.containsKey(table)){
      dataSetObjectMap.get(table)
    }else{
      throw TableLoadException("Stage[%s] | Table[%s] can not be found，check if it loaded".format(currentStage.stageId,table))
    }
  }


  def addDataSet(tableInfo:TableInfo,dataSet:DataFrame): Unit ={
    dataSetObjectMap.put(tableInfo.targetName,dataSet)
    dataSet.registerTempTable(tableInfo.targetName)
  }


}
