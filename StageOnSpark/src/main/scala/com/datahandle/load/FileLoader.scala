package com.datahandle.load

import java.io.File
import java.util

import com.boc.iff.exception.{StageHandleException, StageInfoErrorException}
import com.boc.iff.model.{CDate, CDecimal, CInteger, IFFField, IFFFieldType, IFFSection}
import com.config.SparkJobConfig
import com.context.{FileReadStageRequest, StageAppContext}
import com.log.LogBuilder
import com.model.{FileInfo, TableInfo}
import org.apache.commons.io.FileUtils
import org.apache.commons.lang3.StringUtils
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{DataTypes, StructField}
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.springframework.beans.BeansException
import org.springframework.context.support.GenericXmlApplicationContext
import org.springframework.core.io.ByteArrayResource

import scala.collection.mutable.ArrayBuffer

/**
  * Created by scutlxj on 2017/2/9.
  */
abstract class FileLoader extends Serializable{

  var sparkContext:SparkContext = _
  var sqlContext:SQLContext = _
  var jobConfig:SparkJobConfig = _
  var tableInfo:TableInfo = _
  var fileInfo:FileInfo = _
  var logBuilder:LogBuilder = _

  var fieldDelimiter = "\001"

  def load(fileReadStageRequest:FileReadStageRequest)(implicit stageAppContext: StageAppContext): Unit ={
    sparkContext = stageAppContext.sparkContext
    jobConfig = stageAppContext.jobConfig
    sqlContext = stageAppContext.sqlContext
    logBuilder = stageAppContext.constructLogBuilder()
    this.fileInfo = fileReadStageRequest.fileInfos.get(0)
    tableInfo = loadTableInfo(fileInfo)
    tableInfo.targetName = fileReadStageRequest.stageId


    stageAppContext.addTable(tableInfo)
    val df = loadFile
    stageAppContext.addDataSet(tableInfo,df)
  }

  def loadFile(): DataFrame

  protected def changeRddToDataFrame(rdd:RDD[String]): DataFrame ={
    val fieldDelimiter = this.fieldDelimiter
    val fields: List[IFFField] = tableInfo.getBody.fields.filter(!_.filter)
    val basePk2Map= (x:String) => {
      val rowData = x.split(fieldDelimiter)
      val array = new ArrayBuffer[Any]
      for(v<-rowData){
        array += v
      }
      Row.fromSeq(array)
    }
    val structFields = new util.ArrayList[StructField]()
    for(f <- fields) {
      structFields.add(DataTypes.createStructField(f.name, DataTypes.StringType, true))
    }
    val structType = DataTypes.createStructType(structFields)
    val rddN = rdd.map(basePk2Map)
    sqlContext.createDataFrame(rddN,structType)
  }

  protected def loadTableInfo(fileInfo:FileInfo): TableInfo ={
    if(StringUtils.isEmpty(fileInfo.xmlPath)){
      throw new StageInfoErrorException("Xml file is required")
    }
    val metadataFile = new File(fileInfo.xmlPath)
    var metadataXml = FileUtils.readFileToString(metadataFile, fileInfo.metadataFileEncoding)
    metadataXml = StringUtils.replace(metadataXml, "com.boc.oms.model.", "com.boc.iff.model.")
    metadataXml = StringUtils.replace(metadataXml, "com.boc.isb.util.iff.", "com.boc.iff.")
    val metadataXmlResource = new ByteArrayResource(metadataXml.getBytes(fileInfo.metadataFileEncoding))
    val appContext = new GenericXmlApplicationContext()
    appContext.setValidating(false)
    appContext.load(metadataXmlResource)
    appContext.refresh()

    val tableInfo:TableInfo = new TableInfo
    try {
      tableInfo.targetSchema = appContext.getBean("TargetSchema").asInstanceOf[String]
      tableInfo.targetTable = appContext.getBean("TargetTable").asInstanceOf[String]
      try {
        tableInfo.srcSeparator = appContext.getBean("SrcSeparator").asInstanceOf[String]
      }catch {
        case e:BeansException =>
      }
      try {
        tableInfo.fixedLength = appContext.getBean("fixedLength").asInstanceOf[String]
      }catch {
        case e:BeansException =>
      }
      tableInfo.sourceCharset = appContext.getBean("SrcCharset").asInstanceOf[String]
      val iffSectionClass = classOf[IFFSection]
      tableInfo.header = appContext.getBean("header", iffSectionClass)
      tableInfo.body = appContext.getBean("body", iffSectionClass)
      tableInfo.footer = appContext.getBean("footer", iffSectionClass)
      //tableInfo.targetName = appContext.getBean("TargetTable").asInstanceOf[String]
    }
    catch {
      case e: BeansException =>
        logBuilder.error( fileInfo.xmlPath + " BeansException.")
        throw e
      case e: ClassNotFoundException =>
        logBuilder.error( fileInfo.xmlPath + " ClassNotFoundException.")
        throw e
    }
    tableInfo.dataLineEndWithSeparatorF = fileInfo.dataLineEndWithSeparatorF
    loadFieldTypeInfo(tableInfo)
    tableInfo
  }

  /**
    * 解析 元数据信息中的列数据格式定义
    *
    */
  protected def loadFieldTypeInfo(tableInfo:TableInfo): Unit = {
    for(field<-tableInfo.body.fields.toArray){
      if(field.typeInfo == null) {
        field.typeInfo = IFFFieldType.getFieldType(tableInfo,null,field)
      }
    }
  }

}
