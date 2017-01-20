package com.config

import java.lang.management.ManagementFactory

import com.boc.iff.AppConfig
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/**
  * Created by cvinc on 2016/6/7.
  */

class SparkJobConfig extends AppConfig{

  var configPath: String = ""
  var metadataFilePath: String = ""                                         //XML 描述文件路径
  var metadataFileEncoding: String = "UTF-8"                                //XML 描述文件编码

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
    optionParser.opt[String]("metadata-file-encoding")
      .text("Metadata File Encoding")
      .foreach(this.metadataFileEncoding = _)
  }

  override def toString = {
    val builder = new mutable.StringBuilder(super.toString)
    if(builder.nonEmpty) builder ++= "\n"
    builder ++= "ConfigPath: %s\n".format(configPath.toString)
    builder.toString
  }

}