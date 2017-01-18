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

  override protected def makeOptions(optionParser: scopt.OptionParser[_]) = {
    super.makeOptions(optionParser)
    optionParser.opt[String]("configPath")
      .required()
      .text("ConfigPath")
      .foreach { x=> this.configPath = x.toLowerCase }
  }

  override def toString = {
    val builder = new mutable.StringBuilder(super.toString)
    if(builder.nonEmpty) builder ++= "\n"
    builder ++= "ConfigPath: %s\n".format(configPath.toString)
    builder.toString
  }

}