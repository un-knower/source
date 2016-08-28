package com.boc.iff

import java.util.Properties
import javax.sql.DataSource

import com.boc.iff.DBManager._
import com.boc.iff.model.IFFMetadata



abstract class DBManager(prefix: String, config: Properties) {

  protected val logger = new ECCLogger()

  protected val (driver, url, username, password) = {
    val driver = config.getProperty(s"$prefix.$PROP_NAME_DRIVER")
    val url = config.getProperty(s"$prefix.$PROP_NAME_URL")
    val username = config.getProperty(s"$prefix.$PROP_NAME_USERNAME")
    val password = config.getProperty(s"$prefix.$PROP_NAME_PASSWORD")
    (driver, url, username, password)
  }

  protected val dataSource: DataSource = {
    val dataSourceConfig = new DataSourceConfig()
    dataSourceConfig.driverClassName = driver
    dataSourceConfig.url = url
    dataSourceConfig.username = username
    dataSourceConfig.password = password
    DBUtils.createDataSource(dataSourceConfig)
  }

  def patchIFFConversionConfig(iffConversionConfig: IFFConversionConfig,
                               dbName: String = null,
                               iTableName: String = null): Unit = {}

  /**
    * 为元数据信息补充缺失的列
    */
  def patchMetadataFields(iffMetadata: IFFMetadata,
                          dbName: String = null,
                          iTableName: String = null): Unit

  def patchDBTableFields(iffMetadata: IFFMetadata,
                         dbName: String = null,
                         iTableName: String = null,
                         fTableName: String = null): Unit

}

object DBManager{
  val PROP_ROOT_PREFEX          = "DBManager"
  val PROP_NAME_DBMANAGER_CLASS = "CLASS"
  val PROP_NAME_USERNAME = "USERNAME"
  val PROP_NAME_PASSWORD = "PASSWORD"
  val PROP_NAME_URL      = "URL"
  val PROP_NAME_DRIVER   = "DRIVER"
  val PROP_PASSWORD_UTIL_CLASSPATH = "PASSWORD.UTIL.CLASSPATH"
  val PROP_PASSWORD_UTIL_CLASS     = "PASSWORD.UTIL.CLASS"
  val PROP_PASSWORD_UTIL_METHOD    = "PASSWORD.UTIL.METHOD"
}