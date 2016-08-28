package com.boc.iff

import java.util.Properties

import org.apache.log4j.{Logger, PropertyConfigurator}

class ECCLogger {

  private val logger = Logger.getLogger(this.getClass)

  def configure(prop: Properties): Unit = {
    PropertyConfigurator.configure(prop)
  }

  private def formatMessage(msgID: String, desc: String): String = {
    ECCLoggerConfigurator.logFormat.format(ECCLoggerConfigurator.systemName, msgID, desc.replace('\n',' '))
  }

  def debug(msgID: String, desc: String): Unit = logger.debug(formatMessage(msgID, desc))

  def warn(msgID: String, desc: String): Unit = logger.warn(formatMessage(msgID, desc))

  def info(msgID: String, desc: String): Unit = logger.info(formatMessage(msgID, desc))

  def error(msgID: String, desc: String): Unit = logger.error(formatMessage(msgID, desc))

  def fatal(msgID: String, desc: String): Unit = logger.fatal(formatMessage(msgID, desc))

}

object ECCLoggerConfigurator extends Serializable {
  var systemName: String = ""
  var logFormat = "[%s%s CHK] [%s]"
}