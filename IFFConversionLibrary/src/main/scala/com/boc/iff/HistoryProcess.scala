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
trait HistoryProcess[T<:DataProcessConfig,D] {

  protected var systemName = DEFAULT_SYSTEM_NAME
  //系统名称, 用于写log
  protected var accountDateField = DEFAULT_ACCOUNT_DATE_FIELD
  //会计日列名
  protected var dataProcessConfig: T = _
  protected val dataSourceConfig = new DataSourceConfig()
  protected var targetDataSource: DataSource = null
  protected var fieldDelimiter = DEFAULT_FIELD_DELIMITER
  //列分隔符
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
    if (!checkFileExists(dataProcessConfig.configFilePath) ||
      !checkFileExists(dataProcessConfig.metadataFilePath) ||
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
    fieldDelimiter = prop.getProperty(PROP_NAME_FIELD_DELIMITER, DEFAULT_FIELD_DELIMITER)
    accountDateField = prop.getProperty(PROP_NAME_ACCOUNT_DATE_FIELD, DEFAULT_ACCOUNT_DATE_FIELD)
    ECCLoggerConfigurator.systemName = systemName

    prop.stringPropertyNames().asScala.filter { propertyName =>
      propertyName.startsWith(DBManager.PROP_ROOT_PREFEX + ".")
    }.map { propertyName =>
      val dbManagerProperties = StringUtils.split(propertyName, ".")
      val dbManagerName: String = dbManagerProperties(1)
      val (dbManagerPropertyName, dbManagerPropertyValue) =
        if (dbManagerProperties.length == 2) {
          val name: String = propertyName + "." + DBManager.PROP_NAME_DBMANAGER_CLASS
          val value: String = prop.getProperty(propertyName)
          (name, value)
        } else {
          val name: String = propertyName
          val value: String = prop.getProperty(name)
          (name, value)
        }
      (dbManagerName, (dbManagerPropertyName, dbManagerPropertyValue))
    }.toTraversable.groupBy(_._1).map { x =>
      val dbManagerName = x._1
      val dbManagerProperties = new Properties()
      x._2.foreach { y =>
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
      if (StringUtils.isNotEmpty(passwordUtilClassPath) && StringUtils.isNotEmpty(passwordUtilClass)) {
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

  /**
   * 解析 XML 元数据文件
   *
   * @param metadataFileName XML 元数据文件路径
   * @param encoding         XML 元数据文件编码
   */
  protected def loadMetadata(metadataFileName: String, encoding: String): Unit = {
    val metadataFile = new File(metadataFileName)
    var metadataXml = FileUtils.readFileToString(metadataFile, encoding)
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
      } catch {
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

  //

  /**
   * 处理数据文件, 具体细节留待子类实现
   */
  protected def processFile(): Unit


  /**
   * 解析 元数据信息中的列数据格式定义
   *
   */
  protected def loadFieldTypeInfo(): Unit = {
    for (field <- iffMetadata.body.fields) {
      if (field.typeInfo == null) {
        field.typeInfo = IFFFieldType.getFieldType(iffMetadata, null, field)
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
    if (!checkFilesExists) return false
    loadConfigProp()
    loadMetadata(dataProcessConfig.metadataFilePath, dataProcessConfig.metadataFileEncoding)
    if (StringUtils.isEmpty(dataProcessConfig.dbName)) dataProcessConfig.dbName = iffMetadata.targetSchema
    if (StringUtils.isEmpty(dataProcessConfig.iTableName)) dataProcessConfig.iTableName = iffMetadata.targetTable
    targetDataSource = DBUtils.createDataSource(dataSourceConfig)
    loadFieldTypeInfo()
    true
  }

  /**
   * 执行整个作业
   */
  protected def run(config: T): Unit = {
    this.dataProcessConfig = config
    if (!prepare()) return
    processFile()
    logger.info(MESSAGE_ID_CNV1001, "File HistoryProcess Complete! File: " + config.iffFileInputPath)
  }

  protected def getIncrease(fileName: String,iffMetadata: IFFMetadata): D

  protected def getHistory(fileName: String,iffMetadata: IFFMetadata): D

  protected def diffHistoryAndIncrease(increase:D,history:D,iffMetadata: IFFMetadata): (D,D)

  protected def appendHistory(closeDF:D,openDF:D): Unit

}




  object HistoryProcess {

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
