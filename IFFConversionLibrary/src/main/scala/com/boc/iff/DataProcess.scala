package com.boc.iff

import java.io.{BufferedInputStream, File, FileInputStream}
import java.util.Properties
import java.util.zip.GZIPInputStream
import javax.sql.DataSource
import com.boc.iff.IFFConversion._
import com.boc.iff.model._
import org.apache.commons.io.FileUtils
import org.apache.commons.lang3.StringUtils
import org.springframework.beans.BeansException
import org.springframework.beans.factory.NoSuchBeanDefinitionException
import org.springframework.context.support.GenericXmlApplicationContext
import org.springframework.core.io.ByteArrayResource

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

/**
 * 1. 加载 配置文件、XML元数据文件
 * 2.
 */
trait DataProcess[T<:DataProcessConfig] {

  protected var systemName = DEFAULT_SYSTEM_NAME               //系统名称, 用于写log
  protected var accountDateField = DEFAULT_ACCOUNT_DATE_FIELD  //会计日列名
  protected var dataProcessConfig: T = _
  protected val dataSourceConfig = new DataSourceConfig()
  protected var targetDataSource: DataSource = null

  protected var iffMetadata: IFFMetadata = null

  val logger = new ECCLogger()

  protected val dbManagers = ListBuffer[DBManager]()

  /**
   * 检查文件路径是否存在
   *
   * @param fileName 文件路径
   * @return
   */
  protected def checkFileExists(fileName: String): Boolean
  /**
   * 检查参数中定义的 配置文件、XML 元数据文件和 IFF 文件是否存在
   *
   * @return
   */
  protected def checkFilesExists: Boolean = {
    if(!checkFileExists(dataProcessConfig.configFilePath)||
      !checkFileExists(dataProcessConfig.metadataFilePath)||
      !checkFileExists(dataProcessConfig.iffFileInputPath)) {
      false
    } else {
      true
    }
  }

  /**
   * 加载 配置文件
   */
  protected def loadConfigProp(): Unit = {
    val prop = new Properties()
    prop.load(new FileInputStream(dataProcessConfig.configFilePath))
    logger.configure(prop)
    systemName = prop.getProperty(PROP_NAME_SYSTEM_NAME, DEFAULT_SYSTEM_NAME)
    accountDateField = prop.getProperty(PROP_NAME_ACCOUNT_DATE_FIELD, DEFAULT_ACCOUNT_DATE_FIELD)
    ECCLoggerConfigurator.systemName = systemName

    prop.stringPropertyNames().asScala.filter { propertyName =>
      propertyName.startsWith(DBManager.PROP_ROOT_PREFEX + ".")
    }.map { propertyName =>
      val dbManagerProperties = StringUtils.split(propertyName, ".")
      val dbManagerName: String = dbManagerProperties(1)
      val (dbManagerPropertyName, dbManagerPropertyValue) =
        if(dbManagerProperties.length==2) {
          val name: String = propertyName + "." + DBManager.PROP_NAME_DBMANAGER_CLASS
          val value: String = prop.getProperty(propertyName)
          (name, value)
        } else {
          val name: String = propertyName
          val value: String = prop.getProperty(name)
          (name, value)
        }
      (dbManagerName, (dbManagerPropertyName, dbManagerPropertyValue))
    }.toTraversable.groupBy(_._1).map { x=>
      val dbManagerName = x._1
      val dbManagerProperties = new Properties()
      x._2.foreach{ y=>
        dbManagerProperties.setProperty(y._2._1, y._2._2)
      }
      (dbManagerName, dbManagerProperties)
    }.toSeq.sortBy(_._1).foreach { dbManagerConfig =>
      val dbManagerName = dbManagerConfig._1
      val prefix = DBManager.PROP_ROOT_PREFEX + "." + dbManagerName
      val dbManagerProperties = dbManagerConfig._2
      val dbManagerClassName = dbManagerProperties.getProperty(prefix + "." + DBManager.PROP_NAME_DBMANAGER_CLASS)
      val dbManagerClass = Class.forName(dbManagerClassName)
      val dbManagerConstructor = dbManagerClass.getConstructor(classOf[String], classOf[Properties])
      logger.info(MESSAGE_ID_CNV1001, "DBManager Prefix: " + prefix)
      logger.info(MESSAGE_ID_CNV1001, "DBManager ClassName: " + dbManagerClassName)
      val passwordUtilClassPath = dbManagerProperties.getProperty(prefix + "." + DBManager.PROP_PASSWORD_UTIL_CLASSPATH)
      val passwordUtilClass = dbManagerProperties.getProperty(prefix + "." + DBManager.PROP_PASSWORD_UTIL_CLASS)
      val passwordUtilMethod = dbManagerProperties.getProperty(prefix + "." + DBManager.PROP_PASSWORD_UTIL_METHOD)
      if(StringUtils.isNotEmpty(passwordUtilClassPath) && StringUtils.isNotEmpty(passwordUtilClass)){
        logger.info(MESSAGE_ID_CNV1001, "Password Util Class Path: " + passwordUtilClassPath)
        logger.info(MESSAGE_ID_CNV1001, "Password Util Class: " + passwordUtilClass)
        logger.info(MESSAGE_ID_CNV1001, "Password Util Method: " + passwordUtilMethod)
        try {
          val passwordManager = new PasswordManager(passwordUtilClassPath, passwordUtilClass, passwordUtilMethod)
          //logger.info(MESSAGE_ID_CNV1001, "Password: " + passwordManager.password)
          dbManagerProperties.setProperty(prefix + "." + DBManager.PROP_NAME_PASSWORD, passwordManager.password)
        } catch {
          case t: Throwable =>
            logger.error(MESSAGE_ID_CNV1001, t.getMessage)
        }
      }
      val dbManager = dbManagerConstructor.newInstance(prefix, dbManagerProperties).asInstanceOf[DBManager]
      dbManagers += dbManager
    }
  }



  protected def openFileInputStream(fileName: String): java.io.InputStream = {
    val file = new File(fileName)
    val fileInputStream = new FileInputStream(file)
    fileInputStream
  }

  /**
   * 打开本地文件流
   *
   * @param fileName 文件路径
   * @param isGZip   文件是否 GZip 格式
   * @param readBufferSize 读取缓冲区大小
   * @return
   */
  protected def openIFFFileBufferedInputStream(fileName: String,
                                               isGZip: Boolean,
                                               readBufferSize: Int): BufferedInputStream = {
    val fileInputStream = openFileInputStream(fileName)
    val fileSourceStream =
      isGZip match {
        case true =>
          logger.debug(MESSAGE_ID_CNV1001, "file isGZip = true")
          new GZIPInputStream(fileInputStream)
        case _ =>
          logger.debug(MESSAGE_ID_CNV1001, "file isGZip = false")
          fileInputStream
      }
    new BufferedInputStream(fileSourceStream, readBufferSize)
  }

  protected def getfileLength(fileName: String, isGZip: Boolean): Long = {
    val file = new File(fileName)
    val length: Long =
      if(isGZip) file.length * 10
      else file.length
    length
  }


  /**
   * 解析 XML 元数据文件
   *
   * @param metadataFileName XML 元数据文件路径
   * @param encoding         XML 元数据文件编码
   */
  protected def loadMetadata(metadataFileName: String, encoding: String): Unit = {
    val metadataFile = new File(metadataFileName)
    var metadataXml = FileUtils.readFileToString(metadataFile, encoding)
    metadataXml = StringUtils.replace(metadataXml, "com.boc.oms.model.", "com.boc.iff.model.")
    metadataXml = StringUtils.replace(metadataXml, "com.boc.isb.util.iff.", "com.boc.iff.")
    val metadataXmlResource = new ByteArrayResource(metadataXml.getBytes(encoding))
    val appContext = new GenericXmlApplicationContext()
    appContext.setValidating(false)
    appContext.load(metadataXmlResource)
    appContext.refresh()

    iffMetadata = new IFFMetadata()
    iffMetadata.targetSchema = appContext.getBean("TargetSchema").asInstanceOf[String]
    iffMetadata.targetTable = appContext.getBean("TargetTable").asInstanceOf[String]
    iffMetadata.srcSeparator = appContext.getBean("SrcSeparator").asInstanceOf[String]
    iffMetadata.sourceCharset =
      try {
        appContext.getBean("SrcCharset").asInstanceOf[String]
      }catch {
        case e: NoSuchBeanDefinitionException =>
          logger.info(MESSAGE_ID_CNV1001, "No SrcCharset define in xml :" + metadataFile.getAbsolutePath)
          null.asInstanceOf[String]
      }
    try {
      val iffSectionClass = classOf[IFFSection]
      iffMetadata.header = appContext.getBean("header", iffSectionClass)
      iffMetadata.body = appContext.getBean("body", iffSectionClass)
      iffMetadata.footer = appContext.getBean("footer", iffSectionClass)
    }
    catch {
      case e: BeansException =>
        logger.error(MESSAGE_ID_CNV1001, dataProcessConfig.metadataFilePath + " BeansException.")
        throw e
      case e: ClassNotFoundException =>
        logger.error(MESSAGE_ID_CNV1001, dataProcessConfig.metadataFilePath + " ClassNotFoundException.")
        throw e
    }
  }

//  /**
//   * 解析数据文件结构
//   *
//   * @param iffFileName     文件路径
//   * @param readBufferSize  读取缓冲区大小
//   */
//  protected def loadIFFFileInfo(iffFileName: String, readBufferSize: Int): Unit ={
//    iffFileInfo = new IFFFileInfo()
//    val iffFileBufferedInputStream = openIFFFileBufferedInputStream(iffFileName, iffFileInfo.isGzip, readBufferSize)
//    iffFileBufferedInputStream.mark(0)
//    iffFileBufferedInputStream.reset()
//    val sourceCharsetName =
//      if(StringUtils.isNotEmpty(iffMetadata.sourceCharset)) {
//        iffMetadata.sourceCharset
//      }else{
//        "UTF-8"
//      }
//    iffMetadata.sourceCharset = sourceCharsetName
//    logger.info(MESSAGE_ID_CNV1001, "SrcCharset: " + sourceCharsetName)
//    val charset = IFFUtils.getCharset(sourceCharsetName)
//    val decoder = charset.newDecoder
//    iffFileInfo.recordLength = iffMetadata.header.getLength
//    val buffer = new Array[Byte](iffFileInfo.recordLength)
//    iffFileBufferedInputStream.read(buffer)
//    iffFileBufferedInputStream.close()
//    val lineEsc = false
//    logger.debug(MESSAGE_ID_CNV1001, IFFUtils.byteDecode(decoder, buffer, lineEsc))
//    val headerInd = IFFUtils.byteDecode(decoder, java.util.Arrays.copyOfRange(buffer, 0, 1), lineEsc)
//
//    // read actual REC_LENGTH/IFFID/FILENAME in header
//    for(field<-iffMetadata.header.fields){
//      val value = IFFUtils.byteDecode(decoder,
//        java.util.Arrays.copyOfRange(buffer, field.getStartPos, field.getEndPos + 1), lineEsc)
//      field.name match {
//        case "REC_LENGTH" =>
//          iffFileInfo.recordLength = value.trim.toInt
//        //logger.info(MESSAGE_ID_CNV1001, "iffFile REC_LENGTH = " + iffFileInfo.recordLength)
//        case "IFFID" =>
//          iffFileInfo.iffId = value
//          logger.info(MESSAGE_ID_CNV1001, "iffFile IFFID = " + iffFileInfo.iffId)
//        case "FILENAME" =>
//          iffFileInfo.fileName = value.trim
//          logger.info(MESSAGE_ID_CNV1001, "iffFile FILENAME = " + iffFileInfo.fileName)
//      }
//    }
//  }

  /**
   * 处理数据文件, 具体细节留待子类实现
   */
  protected def processFile(): Unit



  /**
   * 解析 元数据信息中的列数据格式定义
   *
   */
  protected def loadFieldTypeInfo(): Unit = {
    for(field<-iffMetadata.body.fields){
      if(field.typeInfo == null) {
        field.typeInfo = IFFFieldType.getFieldType(iffMetadata,null,field)
      }
    }
  }

  /**
   * 准备阶段：
   * 1. 检查输入文件是否存在
   * 2. 加载配置文件
   * 3. 查询目标表信息
   *
   * @return
   */
  protected def prepare(): Boolean = {
    if(!checkFilesExists) return false
    loadConfigProp()
    loadMetadata(dataProcessConfig.metadataFilePath, dataProcessConfig.metadataFileEncoding)
    if(StringUtils.isEmpty(dataProcessConfig.dbName)) dataProcessConfig.dbName = iffMetadata.targetSchema
    if(StringUtils.isEmpty(dataProcessConfig.iTableName)) dataProcessConfig.iTableName = iffMetadata.targetTable
    targetDataSource = DBUtils.createDataSource(dataSourceConfig)
    loadFieldTypeInfo()
    true
  }

  /**
   * 执行整个作业
   */
  protected def run(iffConversionConfig: T): Unit = {
    this.dataProcessConfig = iffConversionConfig
    if(!prepare()) return
    processFile()
    logger.info(MESSAGE_ID_CNV1001, "File Conversion Complete! File: " + iffConversionConfig.iffFileInputPath)
  }
}

object DataProcess {

  val PROP_NAME_SYSTEM_NAME = "system_name"                 //配置文件中的键：系统名称
  val PROP_NAME_DB_MANAGER = "DB_MANAGER"                   //配置文件中的键：数据库管理类
  val PROP_NAME_DB_DRIVER = "DRIVER"                        //配置文件中的键：数据库驱动
  val PROP_NAME_DB_URL = "URL"                              //配置文件中的键：数据库 URL 路径
  val PROP_NAME_DB_USERNAME = "USERNAME"                    //配置文件中的键：数据库用户名
  val PROP_NAME_DB_PASSWORD = "PASSWORD"                    //配置文件中的键：数据库用户密码

  val DEFAULT_DB_DRIVER = "com.mysql.jdbc.Driver"           //默认值，数据库驱动
  val DEFAULT_DB_MANAGER = "com.boc.iff.HiveDBManager"      //默认值，数据库管理类

  val MESSAGE_ID_CNV1001 = "-CNV1001AAA"

}
