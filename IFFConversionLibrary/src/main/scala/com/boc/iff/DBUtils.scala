package com.boc.iff

import java.sql.{Connection, PreparedStatement, ResultSet}
import javax.sql.DataSource

import org.apache.commons.dbcp.BasicDataSource

import scala.collection.mutable.ListBuffer

/**
  * Created by cvinc on 2016/6/25.
  */
class DataSourceConfig {
  var driverClassName : String = "com.mysql.jdbc.Driver"
  var url : String = ""
  var username: String = ""
  var password : String = ""
  var maxActive : Int = 10
  var initialSize : Int = 1
  var maxIdle : Int = 2
  var minIdle : Int = 2
  var maxOpenPreparedStatements : Int = 5
  var poolPreparedStatements : Boolean = true
  //valiadation SQL,if testOnBorrow is true
  var validationQuery : String = "select count(1) from dual"
  //interval betwwen two Eviction Threads run.unit:Millis
  var timeBetweenEvictionRunsMillis : Int = 180000
  //times to run Eviction Thread to decide wether Evict idle db connection
  var numTestsPerEvictionRun : Int = 1
  //switch to wether test when get connection from DB Pool.default true
  var testOnBorrow : Boolean = true
  //switch to wether test when close connection from DB Pool.default false
  var testOnReturn : Boolean = false
  //swith to wether test when connection is idle.default true
  var testWhileIdle : Boolean = true
  //DB's max waiting time before releasing a connection
  var maxWait : Int = 30000
  //time a connection can be stayed in DB Pool.default 30 minutes
  var minEvictableIdleTimeMillis : Int = 1800000
}

object DBUtils {

  val logger = new ECCLogger()

  def createDataSource(dataSourceConfig: DataSourceConfig): DataSource = {
    val dataSource = new BasicDataSource()
    dataSource.setDriverClassName(dataSourceConfig.driverClassName)
    dataSource.setUrl(dataSourceConfig.url)
    dataSource.setUsername(dataSourceConfig.username)
    dataSource.setPassword(dataSourceConfig.password)
    dataSource.setMaxActive(dataSourceConfig.maxActive)
    dataSource.setInitialSize(dataSourceConfig.initialSize)
    dataSource.setMaxIdle(dataSourceConfig.maxIdle)
    dataSource.setMinIdle(dataSourceConfig.minIdle)
    dataSource.setMaxOpenPreparedStatements(dataSourceConfig.maxOpenPreparedStatements)
    dataSource.setPoolPreparedStatements(dataSourceConfig.poolPreparedStatements)
    dataSource.setValidationQuery(dataSourceConfig.validationQuery)
    dataSource.setTimeBetweenEvictionRunsMillis(dataSourceConfig.timeBetweenEvictionRunsMillis)
    dataSource.setNumTestsPerEvictionRun(dataSourceConfig.numTestsPerEvictionRun)
    dataSource.setTestOnBorrow(dataSourceConfig.testOnBorrow)
    dataSource.setTestOnReturn(dataSourceConfig.testOnReturn)
    dataSource.setTestWhileIdle(dataSourceConfig.testWhileIdle)
    dataSource.setMaxWait(dataSourceConfig.maxWait)
    dataSource.setMinEvictableIdleTimeMillis(dataSourceConfig.minEvictableIdleTimeMillis)
    dataSource
  }

  def executeSql(dataSourceConfig: DataSourceConfig, sql: String)
                (setPreparedStatement:(PreparedStatement=>Unit)):Unit = {
    val dataSource = createDataSource(dataSourceConfig)
    executeSql(dataSource,sql)(setPreparedStatement)
  }

  def executeSql(dataSource: DataSource, sql: String)
                (setPreparedStatement:(PreparedStatement=>Unit)):Unit = {
    var connection:Connection = null
    var preparedStatement:PreparedStatement = null
    try{
      connection = dataSource.getConnection
      preparedStatement = connection.prepareStatement(sql)
      setPreparedStatement(preparedStatement)
      preparedStatement.execute()
    }catch{
      case e:Exception =>
        e.printStackTrace()
        logger.error("-CNV1001", e.getMessage)
    }finally {
      if(preparedStatement!=null){
        try{
          preparedStatement.close()
        }catch{
          case e:Exception=>logger.error("-CNV1001", e.getMessage)
        }
      }
      if(connection!=null){
        try{
          connection.close()
        }catch{
          case e:Exception=>logger.error("-CNV1001", e.getMessage)
        }
      }
    }
  }

  def querySql[T](dataSource: DataSource, sql: String)
                (setPreparedStatement:(PreparedStatement=>Unit),
                 mapRow:(ResultSet=>T)):List[T] = {
    var connection:Connection = null
    var preparedStatement:PreparedStatement = null
    var resultSet:ResultSet = null
    try{
      connection = dataSource.getConnection
      preparedStatement = connection.prepareStatement(sql)
      setPreparedStatement(preparedStatement)
      resultSet = preparedStatement.executeQuery()
      val resultList = ListBuffer[T]()
      while(resultSet.next()){
        val row = mapRow(resultSet)
        resultList += row
      }
      resultList.toList
    }finally {
      if(resultSet!=null){
        try{
          resultSet.close()
        }catch{
          case e:Exception=>logger.error("-CNV1001", e.getMessage)
        }
      }
      if(preparedStatement!=null){
        try{
          preparedStatement.close()
        }catch{
          case e:Exception=>logger.error("-CNV1001", e.getMessage)
        }
      }
      if(connection!=null){
        try{
          connection.close()
        }catch{
          case e:Exception=>logger.error("-CNV1001", e.getMessage)
        }
      }
    }
  }

}
