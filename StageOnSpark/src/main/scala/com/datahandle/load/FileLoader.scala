package com.datahandle.load

import java.io.File
import java.text.SimpleDateFormat
import java.util
import java.lang.Long
import com.boc.iff.exception.StageInfoErrorException
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

  var stageAppContext:StageAppContext = _
  var sparkContext:SparkContext = _
  var sqlContext:SQLContext = _
  var jobConfig:SparkJobConfig = _
  var tableInfo:TableInfo = _
  var logBuilder:LogBuilder = _
  var fileInfo:FileInfo = _
  var fieldDelimiter = "\001"

  def load(fileReadStageRequest:FileReadStageRequest)(implicit stageAppContext: StageAppContext): Unit = {
    this.stageAppContext = stageAppContext;
    sparkContext = stageAppContext.sparkContext
    jobConfig = stageAppContext.jobConfig
    sqlContext = stageAppContext.sqlContext
    logBuilder = stageAppContext.constructLogBuilder()
    this.fileInfo = fileReadStageRequest.fileInfos.get(0)
    if (StringUtils.isNotEmpty(fileInfo.targetSeparator)) {
      this.fieldDelimiter = fileInfo.targetSeparator
    }
    if (StringUtils.isNotEmpty(fileInfo.xmlPath)) {
      tableInfo = loadTableInfo(fileInfo)
      tableInfo.targetName = fileReadStageRequest.stageId
    } else {
      tableInfo = fileReadStageRequest.outputTable
      tableInfo.srcSeparator = fileInfo.targetSeparator
      if (tableInfo.body.fields.last.endPos != null) tableInfo.fixedLength = tableInfo.body.fields.last.endPos.toString
      tableInfo.sourceCharset = fileInfo.sourceCharset
    }
    loadFieldTypeInfo(tableInfo)

    val df = loadFile

    val outPutTable = new TableInfo
    outPutTable.targetName = tableInfo.targetName
    outPutTable.body = new IFFSection
    outPutTable.body.fields = tableInfo.body.fields.filter(!_.filter).map(x => {
      x.name = x.name.toUpperCase;
      x
    })
    this.stageAppContext.addTable(outPutTable)
    this.stageAppContext.addDataSet(outPutTable, df)
    //df.first()
  }

  def loadFile(): DataFrame

  protected def changeRddToDataFrame(rdd:RDD[String]): DataFrame ={
    val fieldDelimiter = this.fieldDelimiter
    val fields: List[IFFField] = tableInfo.getBody.fields.filter(!_.filter)
    val basePk2Map= (x:String) => {
      val rowData = StringUtils.splitByWholeSeparatorPreserveAllTokens(x, fieldDelimiter)
      val array = new ArrayBuffer[Any]
      for (v <- 0 until fields.size) {
       if(StringUtils.isNotEmpty(rowData(v))) {
         array += fields(v).typeInfo match {
            case fieldType: CInteger => new Long(rowData(v))
            case fieldType: CDecimal => new java.lang.Double(rowData(v))
            case fieldType: CDate => new java.sql.Date(new SimpleDateFormat(fieldType.pattern).parse(rowData(v)).getTime)
            case _ => rowData(v)
          }
        }else {
          array += null
        }
      }
      Row.fromSeq(array)
    }

  /*  val pro = new Properties
      pro.load(new FileInputStream(stageAppContext.jobConfig.configPath))
      val mapPartitionFun:(Iterator[String] => Iterator[Row])= { rs =>
      val logger = new ECCLogger
      logger.configure(pro)
      logger.info("FileLoader","*************************FileLoader*****************************************8")
      val resultList = new ListBuffer[Row]
      while(rs.hasNext) {
        val x = rs.next()
        resultList+=basePk2Map(x)
      }
      resultList.iterator
    }*/

    val structFields = new util.ArrayList[StructField]()
    for(f <- fields) {
      val tp = f.typeInfo match {
        case fieldType: CInteger => DataTypes.LongType
        case fieldType: CDecimal => DataTypes.DoubleType
        case fieldType: CDate => DataTypes.DateType
        case _ => DataTypes.StringType
      }
      structFields.add(DataTypes.createStructField(f.name.toUpperCase, tp, true))
    }
    val structType = DataTypes.createStructType(structFields)
    val rddN = rdd.map(basePk2Map)
    sqlContext.createDataFrame(rddN,structType)
  }

  protected def changeRddRowToDataFrame(rdd:RDD[Row]): DataFrame ={
    val fields: List[IFFField] = tableInfo.getBody.fields.filter(!_.filter)
    val structFields = new util.ArrayList[StructField]()
    for(f <- fields) {
      val tp = f.typeInfo match {
        case fieldType: CInteger => DataTypes.LongType
        case fieldType: CDecimal => DataTypes.DoubleType
        case fieldType: CDate => DataTypes.DateType
        case _ => DataTypes.StringType
      }
      structFields.add(DataTypes.createStructField(f.name.toUpperCase, tp, true))
    }
    val structType = DataTypes.createStructType(structFields)
    sqlContext.createDataFrame(rdd,structType)
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

  protected def getErrorPath():String={
    "%s/%s/%s/%s".format(jobConfig.tempDir,"errorRecs",stageAppContext.batchName,stageAppContext.stageEngine.currentStage().stageId)
  }

}
