package com.boc.iff

/**
  * Created by cvinc on 2016/6/27.
  */
trait AppConfig {

  var appName: String = "App"
  var appVersion: String = "1.0"
  var logDetail: Boolean = false
  def parse(args: Array[String]) = {
    new scopt.OptionParser[Unit](this.appName){makeOptions(this)}.parse(args)
  }

  protected def makeOptions(optionParser: scopt.OptionParser[_]):Unit = {
    optionParser.head(appName, appVersion)
    optionParser.opt[Unit]("log-detail")
      .text("Print Detail Log")
      .foreach{ x=> this.logDetail = true }
  }

  override def toString = {
    val builder = new StringBuilder(1024)
    builder ++= "Application Name: %s\n".format(appName)
    builder ++= "Application Version: %s\n".format(appVersion)
    builder.toString
  }
}
