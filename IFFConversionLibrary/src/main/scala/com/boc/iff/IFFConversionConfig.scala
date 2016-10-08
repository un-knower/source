package com.boc.iff

import java.text.SimpleDateFormat
import java.util.Date

import com.boc.iff.IFFConversionConfig._

import scala.collection.mutable

/**
  * Created by cvinc on 2016/6/27.
  */
class IFFConversionConfig extends AppConfig {

  var configFilePath: String = ""                                           //配置文件路径
  var metadataFilePath: String = ""                                         //XML 描述文件路径
  var metadataFileEncoding: String = "UTF-8"                                //XML 描述文件编码
  var iffFileInputPath: String = ""                                         //IFF 文件路径
  var datFileOutputPath: String = ""                                        //转换后文件输出路径
  var accountDate: Date = null                                              //会计日
  var blockSize: Int = 1024 * 1024 * 1024                                   //每次读取文件的块大小, 默认 1G
  var sliceSize: Int = 100 * 1024                                           //每个分片大小, 默认 100K
  var dbName: String = ""                                                   //目标数据库名
  var iTableName: String = ""                                               //目标数据库中增量表名
  var fTableName: String = ""                                               //目标数据库中全量表名
  var autoDeleteTargetDir: Boolean = true                                   //作业开始前是否自动清空目标目录, 默认 清空
  var readBufferSize: Int = 8 * 1024 * 1024                                 //文件读取缓冲区大小, 默认 8M
  var tempDir: String = "/tmp/birdie/IFFConversion"                         //临时目录
  var errorFilDir: String = "/tmp/birdie/Bocsz/Error"                       //错误记录存放目录
  var noPatchSchema: Boolean = false                                        //不进行xml数据列修正和数据库列修正
  var filename: String = ""                                                 //文件名称
  var fileEOFPrefix: String = ""                                            //文件结尾标识符
  var fileMaxError: Long = 100                                               //最大文件错误条数
  var fileMaxBlank: Long = 0                                                 //文件最大空行数
  var fileSystemType: String = "UNIX"                                                 //文件系统类型，UNIX/DOS
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
    optionParser.opt[String]("iff-file-input-path")
      .required()
      .text("IFF File Input Path")
      .foreach(this.iffFileInputPath = _)
    optionParser.opt[String]("dat-file-output-path")
      .text("DAT File Output Path")
      .foreach(this.datFileOutputPath = _)
    optionParser.opt[String]("dat-file-erorr-path")
      .text("DAT File ERROR Output Path")
      .foreach(this.errorFilDir = _)
    optionParser.opt[String]("iff-file-input-filename")
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
    optionParser.opt[Unit]("no-patch-schema")
      .text("No Patch Schema")
      .foreach {x=> this.noPatchSchema = true}
    optionParser.opt[String]("dat-file-eof-prefix")
      .text("DAT File EOF prefix")
      .foreach(this.fileEOFPrefix = _)
    optionParser.opt[Long]("dat-file-fileMaxError-number")
      .text("DAT File fileMaxError Number")
      .foreach(this.fileMaxError = _)
    optionParser.opt[Long]("dat-file-fileMaxBlank-number")
      .text("DAT File fileMaxBlank Number")
      .foreach(this.fileMaxBlank = _)
    optionParser.opt[String]("dat-file-system-type")
      .text("DAT File System type")
      .foreach(this.fileSystemType = _)
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
    builder ++= "No Patch Schema: %s".format(String.valueOf(noPatchSchema))
    builder.toString
  }
}

object IFFConversionConfig {
  val ACCOUNT_DATE_PATTERN = "yyyyMMdd"  //常量，会计日日期格式
}