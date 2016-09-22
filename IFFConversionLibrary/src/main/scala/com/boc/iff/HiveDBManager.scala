package com.boc.iff

import java.util.Properties

import com.boc.iff.model.{IFFField, IFFMetadata}
import org.apache.commons.lang3.StringUtils
import HiveDBManager._
import scala.collection.mutable.ListBuffer

/**
  * Created by cvinc on 2016/6/28.
  */

case class HiveDBTable(dbName: String, tableName: String, cdId: Long, location: String)
case class HiveDBField(table: HiveDBTable, fieldName: String, fieldType: String)

class HiveDBManager(prefix: String, config: Properties)
  extends DBManager(prefix: String, config: Properties) {

  protected val autoDeleteTargetDir: Boolean =
    config.getProperty(s"$prefix.$PROP_NAME_AUTO_DELETE_TARGET_DIR", "Y").equals("Y")

  override def patchIFFConversionConfig(iffConversionConfig: IFFConversionConfig,
                                        dbName: String,
                                        iTableName: String): Unit = {
    val iTable =getDBTable(iffConversionConfig.dbName, iffConversionConfig.iTableName)
    if (StringUtils.isEmpty(iffConversionConfig.datFileOutputPath)) {
      iffConversionConfig.datFileOutputPath = iTable.location
    }
    iffConversionConfig.autoDeleteTargetDir = autoDeleteTargetDir
  }

  def patchMetadataFields(iffMetadata: IFFMetadata,
                          dbName: String = null,
                          iTableName: String = null): Unit = {

    val table = getDBTable(dbName, iTableName)
    val tableFieldNameList = getDBTableFields(table).map(_.fieldName.toLowerCase)
    val metadataFieldNameList = iffMetadata.body.fields.map(_.name.toLowerCase)
    val fieldsToFilter = ListBuffer[(String, Int)]()
    var iTableFieldNameIndex: Int = 0
    for(iTableFieldName<-tableFieldNameList){
      if(!metadataFieldNameList.contains(iTableFieldName)){
        fieldsToFilter += ((iTableFieldName, iTableFieldNameIndex))
      }
      iTableFieldNameIndex += 1
    }
    for((fieldName, index)<-fieldsToFilter){
      logger.info("-CNV1001", "Patch Field: %s, %d".format(fieldName, index))
      val field = new IFFField()
      field.name = fieldName
      field.`type` = "string"
      field.filler = true
      iffMetadata.body.fields = iffMetadata.body.fields.patch(index, Array[IFFField](field), 0)
    }
  }

  def patchDBTableFields(iffMetadata: IFFMetadata,
                         dbName: String = null,
                         iTableName: String = null,
                         fTableName: String = null): Unit = {
    val iTable = getDBTable(dbName, iTableName)
    patchDBTableFields(iffMetadata, iTable)
    if(StringUtils.isNotEmpty(fTableName)){
      val fTable = getDBTable(dbName, fTableName)
      patchDBTableFields(iffMetadata, fTable)
    }
  }

  /**
    * 为 目标数据库 表补充缺失的列
    *
    * @param iffMetadata   元数据
    * @param table         目标表
    */
  protected def patchDBTableFields(iffMetadata: IFFMetadata, table: HiveDBTable): Unit = {
    val metadataFieldNamesInLowerCase = iffMetadata.body.fields.map(_.name.toLowerCase)
    val tableFieldList = getDBTableFields(table)
    val tableFieldNameList = tableFieldList.map(_.fieldName.toLowerCase)
    val fieldsToAdd = ListBuffer[HiveDBField]()
    for (metadataFieldName <- metadataFieldNamesInLowerCase) {
      if (!tableFieldNameList.contains(metadataFieldName)) {
        fieldsToAdd += HiveDBField(table, metadataFieldName, "string")
      }
    }
    if (fieldsToAdd.nonEmpty) {
      appendDBTableFields(table, fieldsToAdd)
      logger.info("-CNV1001",
        "Append Field: DBName=%s, TableName=%s, Fields=%s".format(table.dbName, table.tableName,
          fieldsToAdd.map(_.fieldName).mkString(",")))
    }
  }

  def getDBTable(dbName: String, tableName: String): HiveDBTable = {
    val sql =
      """
        |SELECT DBS.NAME, TBLS.TBL_NAME, CD_ID, LOCATION
        | FROM SDS
        | JOIN TBLS ON SDS.SD_ID=TBLS.SD_ID
        | JOIN DBS ON TBLS.DB_ID=DBS.DB_ID
        | WHERE DBS.NAME=?
        | AND TBLS.TBL_NAME=?
      """.stripMargin
    logger.info("dbArg","dbarg:dbName="+dbName+",tableName="+tableName)
    DBUtils.querySql(dataSource, sql)({ pstmt=>
      pstmt.setString(1, dbName)
      pstmt.setString(2, tableName)
    },{ resultSet=>
      HiveDBTable(resultSet.getString(1),
        resultSet.getString(2),
        resultSet.getLong(3),
        resultSet.getString(4))
    }).head
  }

  def getDBTableFields(table: HiveDBTable): Seq[HiveDBField] = {
    val sql = """
                | SELECT COLUMNS_V2.COLUMN_NAME, COLUMNS_V2.TYPE_NAME
                | FROM COLUMNS_V2
                | JOIN SDS ON COLUMNS_V2.CD_ID=SDS.CD_ID
                | JOIN TBLS ON SDS.SD_ID=TBLS.SD_ID
                | JOIN DBS ON TBLS.DB_ID=DBS.DB_ID
                | WHERE DBS.NAME=?
                | AND TBLS.TBL_NAME=?
                | ORDER BY COLUMNS_V2.CD_ID ASC, COLUMNS_V2.INTEGER_IDX ASC
              """.stripMargin
    DBUtils.querySql(dataSource,sql)({ pstmt=>
      pstmt.setString(1, table.dbName)
      pstmt.setString(2, table.tableName)
    },{ resultSet=>
      HiveDBField(table, resultSet.getString(1), resultSet.getString(2))
    })
  }

  def appendDBTableFields(table: HiveDBTable, fields: Seq[HiveDBField]): Unit = {
    val orginialTableFields = getDBTableFields(table)
    var fieldIndex = orginialTableFields.size
    val sb = new StringBuilder()
    sb ++= "INSERT INTO COLUMNS_V2 (CD_ID, COLUMN_NAME, TYPE_NAME, INTEGER_IDX) VALUES "
    var first = true
    for(field<-fields){
      if(first) first = false
      else sb ++= ","
      sb ++= "(?,?,?,?)"
    }
    val sql = sb.toString
    DBUtils.executeSql(dataSource, sql) { pstmt=>
      var rowIndex: Int = 0
      for(field<-fields){
        pstmt.setLong(4 * rowIndex + 1, table.cdId)
        pstmt.setString(4 * rowIndex + 2, field.fieldName)
        pstmt.setString(4 * rowIndex + 3, field.fieldType)
        pstmt.setInt(4 * rowIndex + 4, fieldIndex)
        fieldIndex += 1
        rowIndex += 1
      }
    }
  }
}

object HiveDBManager {
  val PROP_NAME_AUTO_DELETE_TARGET_DIR = "autoDeleteTargetDir"
}
