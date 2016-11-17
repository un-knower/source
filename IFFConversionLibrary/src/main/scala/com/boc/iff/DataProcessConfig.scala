package com.boc.iff

import java.text.SimpleDateFormat
import java.util.Date
import com.boc.iff.DataProcessConfig._
import scala.collection.mutable

/**
 * Created by Clevo on 2016/9/18.
  * @author www.birdiexx.com
 */
class DataProcessConfig extends IFFConversionConfig{
  private val dateFormat = new SimpleDateFormat(ACCOUNT_DATE_PATTERN)
  var iTableDatFilePath: String = ""                                        //增量表数据目录
  var fTableName: String = ""                                               //目标数据库中全量表名
  var fTableDatFilePath: String = ""                                        //全量表数据目录

  /**
   * 程序命令行参数定义及解析
   *
   * @param optionParser
   */
  override protected def makeOptions(optionParser: scopt.OptionParser[_]) = {
    super.makeOptions(optionParser)
    optionParser.opt[String]("f-table-name")
      .required()
      .text("F Table Name")
      .foreach { x=> this.fTableName = x.toLowerCase }
  }
  override def toString = {
    val builder = new mutable.StringBuilder(super.toString)
    if(builder.nonEmpty) builder ++= "\n"
    builder ++= "Database Name: %s\n".format(dbName)
    builder ++= "I Table Name: %s\n".format(iTableName)
    builder ++= "F Table Name: %s\n".format(fTableName)
    builder ++= "Temp Dir: %s\n".format(tempDir)
    builder ++= "Block Size: %d\n".format(blockSize)
    builder ++= "Slice Size: %d\n".format(sliceSize)
    builder ++= "Account Date: %s\n".format(dateFormat.format(accountDate))
    builder ++= "Config File Path: %s\n".format(configFilePath)
    builder ++= "Metadata File Path: %s\n".format(metadataFilePath)
    builder ++= "Metadata File Encoding: %s\n".format(metadataFileEncoding)
    builder ++= "IFF File Input Path: %s\n".format(iffFileInputPath)
    builder ++= "DAT File Output Path: %s\n".format(datFileOutputPath)
    builder.toString
  }
}

object DataProcessConfig {
  val ACCOUNT_DATE_PATTERN = "yyyyMMdd"  //常量，日期格式
}
