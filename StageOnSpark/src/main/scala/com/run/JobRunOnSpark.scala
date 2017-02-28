package com.run

import com.boc.iff.exception._
import com.config.SparkJobConfig
import com.core.AppInit
import org.apache.commons.lang3.StringUtils

/**
  *  Spark 程序入口
  *  @author www.birdiexx.com
  */
object JobRunOnSpark extends App{
  val config = new SparkJobConfig()
  val job = new AppInit[SparkJobConfig]()
  val logger = job.logBuilder
  try {
    job.start(config, args)
  } catch {
    case e: StageInfoErrorException =>
      e.printStackTrace()
      if(StringUtils.isNotEmpty(e.getMessage)) {
        logger.info( e.getMessage)
        logger.error( e.getMessage)
      }
      System.exit(2)
    case e: StageHandleException =>
      e.printStackTrace()
      if(StringUtils.isNotEmpty(e.getMessage)) {
        logger.info( e.getMessage)
        logger.error( e.getMessage)
      }
      System.exit(3)
    case t: Throwable =>
      t.printStackTrace()
      if(StringUtils.isNotEmpty(t.getMessage)) logger.error( t.getMessage)
      System.exit(9)
  }
}
