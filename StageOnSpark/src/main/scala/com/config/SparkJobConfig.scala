package com.config

import java.lang.management.ManagementFactory

import com.boc.iff.{AppConfig, IFFUtils}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/**
  * Created by cvinc on 2016/6/7.
  */

class SparkJobConfig extends AppConfig with Serializable{

  var configPath: String = ""
  var metadataFilePath: String = ""                                         //XML 描述文件路径
  var metadataFileEncoding: String = "UTF-8"                                //XML 描述文件编码
  var blockSize: Int = 200 * 1024 * 1024                                   //每次读取文件的块大小, 默认 200M
  var iffNumberOfThread:Int = 0                                                //程序线程数
  var tempDir:String = "/tmp/birdie/sparklet"                                                //程序线程数
  var debug:Boolean = false
  var defaultDebugFilePath:String = "/tmp/birdie/sparklet/debug"                                                //程序线程数

  override protected def makeOptions(optionParser: scopt.OptionParser[_]) = {
    super.makeOptions(optionParser)
    optionParser.opt[String]("configPath")
      .required()
      .text("ConfigPath")
      .foreach { x=> this.configPath = x }
    optionParser.opt[String]("metadata-file-path")
      .required()
      .text("Metadata File Path")
      .foreach(this.metadataFilePath = _)
    optionParser.opt[String]("temp-dir")
      .text("Temp Dir")
      .foreach(this.tempDir = _)
    optionParser.opt[String]("metadata-file-encoding")
      .text("Metadata File Encoding")
      .foreach(this.metadataFileEncoding = _)
    optionParser.opt[String]("block-size")
      .text("blockSize")
      .foreach(x=>this.blockSize = IFFUtils.getSize(x))
    optionParser.opt[Int]("iff-number-of-thread")
      .text("iffNumberOfThread")
      .foreach(this.iffNumberOfThread = _)
    optionParser.opt[Int]("debug")
      .text("debug")
      .foreach(x=>if("Y".equals(x))true else false)
  }

  override def toString = {
    val builder = new mutable.StringBuilder(super.toString)
    if(builder.nonEmpty) builder ++= "\n"
    builder ++= "ConfigPath: %s\n".format(configPath.toString)
    builder.toString
  }

}