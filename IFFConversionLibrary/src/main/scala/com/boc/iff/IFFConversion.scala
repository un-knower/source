package com.boc.iff

import java.io.{BufferedInputStream, File, FileInputStream}
import java.text.SimpleDateFormat
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
  * 2. 修正元数据及目标表结构
  *   2.1 如果元数据中没有定义会计日，则在元数据第一列补上
  *   2.2 如果元数据中缺少了表中已定义的列，则在元数据中补上，并设置为以空白填充
  *   2.3 如果元数据中的列比表定义的列多，则修改表结构，补上缺失的列
  * 3. 根据元数据定义，把 IFF 文件转换为 UTF-8 编码，去掉头尾，并放入指定的位置
  *
  */
trait IFFConversion[T<:IFFConversionConfig] {

  protected var systemName = DEFAULT_SYSTEM_NAME               //系统名称, 用于写log
  protected var accountDateField = DEFAULT_ACCOUNT_DATE_FIELD  //会计日列名
  protected var addAccountDate = false                         //是否自动补上会计日列
  protected var fieldDelimiter = DEFAULT_FIELD_DELIMITER       //列分隔符
  protected var targetCharset = DEFAULT_TARGET_CHARSET         //目标字符集
  protected var numberOfThread = DEFAULT_NUMBER_OF_THREAD      //执行转换时的并行线程数

  protected var iffConversionConfig: T = _
  protected val dataSourceConfig = new DataSourceConfig()
  protected var targetDataSource: DataSource = null

  protected var iffMetadata: IFFMetadata = null
  protected var iffFileInfo: IFFFileInfo = null

  protected var specialCharConvertor:SpecialCharConvertor = null

  val logger = new ECCLogger()

  protected val dbManagers = ListBuffer[DBManager]()

  /**
    * 检查本地文件路径是否存在
    *
    * @param fileName 文件路径
    * @return
    */
  protected def checkLocalFileExists(fileName: String): Boolean = {
    val file = new File(fileName)
    if(!file.exists()) {
      logger.error(MESSAGE_ID_CNV1001, "File 	:" + file.getAbsolutePath + " not exists.")
      false
    }else true
  }

  /**
    * 检查文件路径是否存在
    *
    * @param fileName 文件路径
    * @return
    */
  protected def checkFileExists(fileName: String): Boolean = {
    checkLocalFileExists(fileName)
  }

  /**
    * 检查参数中定义的 配置文件、XML 元数据文件和 IFF 文件是否存在
    *
    * @return
    */
  protected def checkFilesExists: Boolean = {
    if(!checkFileExists(iffConversionConfig.configFilePath)||
      !checkFileExists(iffConversionConfig.metadataFilePath)||
      !checkFileExists(iffConversionConfig.iffFileInputPath)) {
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
    prop.load(new FileInputStream(iffConversionConfig.configFilePath))
    logger.configure(prop)
    fieldDelimiter = prop.getProperty(PROP_NAME_FIELD_DELIMITER, DEFAULT_FIELD_DELIMITER)
    targetCharset = prop.getProperty(PROP_NAME_TARGET_CHARSET, DEFAULT_TARGET_CHARSET)
    systemName = prop.getProperty(PROP_NAME_SYSTEM_NAME, DEFAULT_SYSTEM_NAME)
    addAccountDate = prop.getProperty(PROP_NAME_ADD_ACCOUNT_DATE, "Y").toUpperCase == "Y"
    accountDateField = prop.getProperty(PROP_NAME_ACCOUNT_DATE_FIELD, DEFAULT_ACCOUNT_DATE_FIELD)
    numberOfThread = prop.getProperty(PROP_NAME_NUMBER_OF_THREAD, DEFAULT_NUMBER_OF_THREAD.toString).toInt
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

  /**
    * 识别文件是否 GZip 格式
    *
    * @param fileName 文件路径
    * @return
    */
  protected def checkGZipFile(fileName: String): Boolean = {
    //val zipIndicator = Array[Byte](80, 75, 0x3, 0x4)
    val gzipIndicator = Array[Byte](31, -117)
    val fileInputStream = openIFFFileInputStream(fileName)
    var bufferedInputStream = new BufferedInputStream(fileInputStream, gzipIndicator.length)
    var isGZip= false
    val magicHeader: Array[Byte] = new Array[Byte](gzipIndicator.length)
    bufferedInputStream.read(magicHeader, 0, gzipIndicator.length)
    if (java.util.Arrays.equals(magicHeader, gzipIndicator)) {
      isGZip = true
    }
    bufferedInputStream.close()
    bufferedInputStream = null
    isGZip
  }

  protected def openIFFFileInputStream(fileName: String): java.io.InputStream = {
    val iffFile = new File(fileName)
    val iffFileInputStream = new FileInputStream(iffFile)
    iffFileInputStream
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
    val iffFileInputStream = openIFFFileInputStream(fileName)
    val iffFileSourceStream =
      isGZip match {
        case true =>
          logger.debug(MESSAGE_ID_CNV1001, "iffFile isGZip = true")
          new GZIPInputStream(iffFileInputStream)
        case _ =>
          logger.debug(MESSAGE_ID_CNV1001, "iffFile isGZip = false")
          iffFileInputStream
      }
    new BufferedInputStream(iffFileSourceStream, readBufferSize)
  }

  protected def getIFFFileLength(fileName: String, isGZip: Boolean): Long = {
    val file = new File(fileName)
    val length: Long =
      if(isGZip) file.length * 10
      else file.length
    length
  }

  /**
    * 识别文件是否主机格式
    *
    * @param fileName 文件路径
    * @param isGZip   是否 GZip 格式
    * @return
    */
  def checkHostFile(fileName: String, isGZip: Boolean): Boolean = {
    val hostIndicator = Array[Byte](-16)
    var isHost = false
    val magic_header = new Array[Byte](hostIndicator.length)
    val iffInputStream = openIFFFileBufferedInputStream(fileName, isGZip, hostIndicator.length)
    iffInputStream.read(magic_header, 0, hostIndicator.length)
    iffInputStream.close()
    if (java.util.Arrays.equals(magic_header, hostIndicator)) {
      isHost = true
    }
    isHost
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
    iffMetadata.fixedLength = try {
      appContext.getBean("fixedLength").asInstanceOf[String]
    }catch {
      case e: NoSuchBeanDefinitionException =>
        logger.info(MESSAGE_ID_CNV1001, "No SrcCharset define in xml :" + metadataFile.getAbsolutePath)
        null.asInstanceOf[String]
    }
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
        logger.error(MESSAGE_ID_CNV1001, iffConversionConfig.metadataFilePath + " BeansException.")
        throw e
      case e: ClassNotFoundException =>
        logger.error(MESSAGE_ID_CNV1001, iffConversionConfig.metadataFilePath + " ClassNotFoundException.")
        throw e
    }
  }

  /**
    * 解析 IFF 数据文件结构
    *
    * @param iffFileName     IFF 文件路径
    * @param readBufferSize  读取缓冲区大小
    */
  protected def loadIFFFileInfo(iffFileName: String, readBufferSize: Int): Unit ={
    iffFileInfo = new IFFFileInfo()
    iffFileInfo.gzip = checkGZipFile(iffFileName)
    val iffFileBufferedInputStream = openIFFFileBufferedInputStream(iffFileName, iffFileInfo.isGzip, readBufferSize)
    iffFileBufferedInputStream.mark(0)
    iffFileInfo.host = checkHostFile(iffFileName, iffFileInfo.isGzip)
    iffFileBufferedInputStream.reset()
    val sourceCharsetName =
      if(StringUtils.isNotEmpty(iffMetadata.sourceCharset)) {
        iffMetadata.sourceCharset
      }else{
        if(iffFileInfo.isHost) "boc61388"
        else "UTF-8"
      }
    iffMetadata.sourceCharset = sourceCharsetName
    logger.info(MESSAGE_ID_CNV1001, "SrcCharset: " + sourceCharsetName)
    val charset = IFFUtils.getCharset(sourceCharsetName)
    val decoder = charset.newDecoder
    // record start position is not include record indicator. so 1 + header.getLength()
    iffFileInfo.recordLength = iffMetadata.header.getLength
    val buffer = new Array[Byte](iffFileInfo.recordLength)
    iffFileBufferedInputStream.read(buffer)
    iffFileBufferedInputStream.close()
    val lineEsc = false
    logger.debug(MESSAGE_ID_CNV1001, IFFUtils.byteDecode(decoder, buffer, lineEsc))
    // String headerInd = new String(buffer, 0, 1, charset);
    val headerInd = IFFUtils.byteDecode(decoder, java.util.Arrays.copyOfRange(buffer, 0, 1), lineEsc)
    /*if (!(headerInd == HEADER_INDICATOR)) {
      throw new Exception("iffFile is invalid ,HEADER_INDICATOR not found.")
    }*/
    // read actual REC_LENGTH/IFFID/FILENAME in header
    for(field<-iffMetadata.header.fields){
      val value = IFFUtils.byteDecode(decoder,
        java.util.Arrays.copyOfRange(buffer, field.getStartPos, field.getEndPos + 1), lineEsc)
      field.name match {
        case "REC_LENGTH" =>
          iffFileInfo.recordLength = value.trim.toInt
        //logger.info(MESSAGE_ID_CNV1001, "iffFile REC_LENGTH = " + iffFileInfo.recordLength)
        case "IFFID" =>
          iffFileInfo.iffId = value
          logger.info(MESSAGE_ID_CNV1001, "iffFile IFFID = " + iffFileInfo.iffId)
        case "FILENAME" =>
          iffFileInfo.fileName = value.trim
          logger.info(MESSAGE_ID_CNV1001, "iffFile FILENAME = " + iffFileInfo.fileName)
      }
    }
    iffFileInfo.recordLength = math.max(iffFileInfo.recordLength, iffMetadata.body.getLength)
    iffFileInfo.recordLength = math.max(iffFileInfo.recordLength, iffMetadata.footer.getLength)
    if(iffMetadata.fixedLength!=null) {
      iffFileInfo.recordLength = math.max(iffFileInfo.recordLength, iffMetadata.fixedLength.toInt)
    }
    logger.info(MESSAGE_ID_CNV1001, "iffFile REC_LENGTH = " + iffFileInfo.recordLength)
    iffFileInfo.fileLength = getIFFFileLength(iffFileName, iffFileInfo.gzip)
  }

  /**
    * 转换 IFF 数据文件, 具体细节留待子类实现
    */
  protected def convertFile(): Unit

  /**
    * 在元数据信息中补充会计日列
    *
    * @param accountDate 会计日
    */
  protected def patchAccountDateField(accountDate: String): Unit = {
    val hasAcdate = iffMetadata.body.fields.exists {
      field => accountDateField.equalsIgnoreCase(field.name)
    }
    hasAcdate match {
      case false =>
        val field = new IFFField()
        field.name = accountDateField
        field.`type` = "string"
        field.constant = true
        field.defaultValue = accountDate
        iffMetadata.body.fields = field::iffMetadata.body.fields
      case _ =>
    }
  }

  /**
    * 解析 元数据信息中的列数据格式定义
    *
    */
  protected def loadIFFFieldTypeInfo(): Unit = {
    for(field<-iffMetadata.body.fields){
      if(field.typeInfo == null) {
        field.typeInfo = IFFFieldType.getFieldType(iffMetadata, iffFileInfo, field)
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
    loadMetadata(iffConversionConfig.metadataFilePath, iffConversionConfig.metadataFileEncoding)
    if(StringUtils.isEmpty(iffConversionConfig.dbName)) iffConversionConfig.dbName = iffMetadata.targetSchema
    if(StringUtils.isEmpty(iffConversionConfig.iTableName)) iffConversionConfig.iTableName = iffMetadata.targetTable
    targetDataSource = DBUtils.createDataSource(dataSourceConfig)
    loadIFFFileInfo(iffConversionConfig.iffFileInputPath, iffConversionConfig.readBufferSize)
    loadIFFFieldTypeInfo()
    if("Y".equals(iffConversionConfig.specialCharConvertFlag)){
      this.specialCharConvertor = new SpecialCharConvertor(iffConversionConfig.specialCharFilePath)
    }
    true
  }

  /**
    * 执行整个作业
    */
  protected def run(iffConversionConfig: T): Unit = {
    this.iffConversionConfig = iffConversionConfig
    if(!prepare()) return
    /*val dateFormat = new SimpleDateFormat(IFFConversionConfig.ACCOUNT_DATE_PATTERN)
    patchAccountDateField(dateFormat.format(iffConversionConfig.accountDate))
    if(!iffConversionConfig.noPatchSchema && dbManagers.nonEmpty){
      dbManagers.head.patchMetadataFields(iffMetadata, iffConversionConfig.dbName, iffConversionConfig.iTableName)
      for(dbManager<-dbManagers){
        dbManager.patchIFFConversionConfig(iffConversionConfig)
        dbManager.patchDBTableFields(iffMetadata,
          iffConversionConfig.dbName, iffConversionConfig.iTableName, iffConversionConfig.fTableName)
      }
    }*/
    if(dbManagers.nonEmpty){
      for(dbManager<-dbManagers){
        dbManager.patchIFFConversionConfig(iffConversionConfig)
      }
    }

    convertFile()
    logger.info(MESSAGE_ID_CNV1001, "File Conversion Complete! File: " + iffConversionConfig.iffFileInputPath)
  }
}

object IFFConversion {

  val PROP_NAME_SYSTEM_NAME = "system_name"                 //配置文件中的键：系统名称
  val PROP_NAME_ACCOUNT_DATE_FIELD = "account_date_field"   //配置文件中的键：会计日列名
  val PROP_NAME_ADD_ACCOUNT_DATE = "add_account_date"       //配置文件中的键：是否补充会计日列
  val PROP_NAME_FIELD_DELIMITER = "field_delimiter"         //配置文件中的键：列分隔符
  val PROP_NAME_TARGET_CHARSET = "tgt_charset"              //配置文件中的键：目标字符集
  val PROP_NAME_NUMBER_OF_THREAD = "number_of_thread"       //配置文件中的键：并行转换线程数

  val PROP_NAME_DB_MANAGER = "DB_MANAGER"                   //配置文件中的键：数据库管理类
  val PROP_NAME_DB_DRIVER = "DRIVER"                        //配置文件中的键：数据库驱动
  val PROP_NAME_DB_URL = "URL"                              //配置文件中的键：数据库 URL 路径
  val PROP_NAME_DB_USERNAME = "USERNAME"                    //配置文件中的键：数据库用户名
  val PROP_NAME_DB_PASSWORD = "PASSWORD"                    //配置文件中的键：数据库用户密码

  val DEFAULT_SYSTEM_NAME = "OMS"                           //默认值，系统名称
  val DEFAULT_ACCOUNT_DATE_FIELD = "ACDATE"                 //默认值，会计日列名
  val DEFAULT_FIELD_DELIMITER = "\001"                      //默认值，列分隔符
  val DEFAULT_TARGET_CHARSET = "UTF-8"                      //默认值，目标字符集
  val DEFAULT_NUMBER_OF_THREAD: Int = 8                     //默认值，并行转换线程数

  val DEFAULT_DB_DRIVER = "com.mysql.jdbc.Driver"           //默认值，数据库驱动
  val DEFAULT_DB_MANAGER = "com.boc.iff.HiveDBManager"      //默认值，数据库管理类

  val HEADER_INDICATOR: String = "0"                        //常量，IFF 文件 Header 行首标识
  val BODY_INDICATOR: String = "1"                          //常量，IFF 文件 Body   行首标识
  val FOOTER_INDICATOR: String = "2"                        //常量，IFF 文件 Footer 行首标识
  
  val MESSAGE_ID_CNV1001 = "-CNV1001"

}
