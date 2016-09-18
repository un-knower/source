package com.boc.iff

import java.text.SimpleDateFormat
import java.util.Date
import com.boc.iff.DataProcessConfig._
import scala.collection.mutable

/**
 * Created by Clevo on 2016/9/18.
 */
class DataProcessConfig extends AppConfig{
  var configFilePath: String = ""                                           //配置文件路径
  var metadataFilePath: String = ""                                         //XML 描述文件路径
  var metadataFileEncoding: String = "UTF-8"                                //XML 描述文件编码
  var dataFileInputPath: String = ""                                         //IFF 文件路径
  var dataFileOutputPath: String = ""                                        //转换后文件输出路径
  var accountDate: Date = null                                              //会计日
  var blockSize: Int = 1024 * 1024 * 1024                                   //每次读取文件的块大小, 默认 1G
  var sliceSize: Int = 100 * 1024                                           //每个分片大小, 默认 100K
  var dbName: String = ""                                                   //目标数据库名
  var iTableName: String = ""                                               //目标数据库中增量表名
  var fTableName: String = ""                                               //目标数据库中全量表名
  var autoDeleteTargetDir: Boolean = true                                   //作业开始前是否自动清空目标目录, 默认 清空
  var readBufferSize: Int = 8 * 1024 * 1024                                 //文件读取缓冲区大小, 默认 8M
  var tempDir: String = "/tmp/birdie/dataprocess"                           //临时目录
  var filename: String = ""                                                 //文件名称
  private val dateFormat = new SimpleDateFormat(ACCOUNT_DATE_PATTERN)

  /**
   * 程序命令行参数定义及解析
   *
   * @param optionParser
   */
  override protected def makeOptions(optionParser: scopt.OptionParser[_]) = {
    super.makeOptions(optionParser)

    optionParser.opt[String]("account-date")
      .required()
      .text("Account Date")
      .foreach{ x=>this.accountDate = dateFormat.parse(x) }
    optionParser.opt[String]("block-size")
      .text("Block Size")
      .foreach { x=>this.blockSize = IFFUtils.getSize(x) }
    optionParser.opt[String]("slice-size")
      .text("Slice Size")
      .foreach { x=>this.sliceSize = IFFUtils.getSize(x) }
    optionParser.opt[String]("config-file-path")
      .required()
      .text("Config File Path")
      .foreach(this.configFilePath = _)
    optionParser.opt[String]("metadata-file-path")
      .required()
      .text("Metadata File Path")
      .foreach(this.metadataFilePath = _)
    optionParser.opt[String]("metadata-file-encoding")
      .text("Metadata File Encoding")
      .foreach(this.metadataFileEncoding = _)
    optionParser.opt[String]("dat-file-input-path")
      .required()
      .text("DAT File Input Path")
      .foreach(this.dataFileInputPath = _)
    optionParser.opt[String]("dat-file-output-path")
      .text("DAT File Output Path")
      .foreach(this.dataFileOutputPath = _)
    optionParser.opt[String]("dat-file-input-filename")
      .text("DAT File Name")
      .foreach(this.filename = _)
    optionParser.opt[String]("db-name")
      .required()
      .text("Database Name")
      .foreach { x=> this.dbName = x.toLowerCase }
    optionParser.opt[String]("i-table-name")
      .required()
      .text("I Table Name")
      .foreach { x=> this.iTableName = x.toLowerCase }
    optionParser.opt[String]("f-table-name")
      .text("F Table Name")
      .foreach { x=> this.fTableName = x.toLowerCase }
    optionParser.opt[Unit]("auto-delete-target-dir")
      .text("Auto Delete Target Dir Before Job")
      .foreach {x=> this.autoDeleteTargetDir = true}
    optionParser.opt[String]("temp-dir")
      .text("Temp Dir")
      .foreach {x=> this.tempDir = x}
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
    builder ++= "IFF File Input Path: %s\n".format(dataFileInputPath)
    builder ++= "DAT File Output Path: %s\n".format(dataFileOutputPath)
    builder.toString
  }
}

object DataProcessConfig {
  val ACCOUNT_DATE_PATTERN = "yyyyMMdd"  //常量，日期格式
}
