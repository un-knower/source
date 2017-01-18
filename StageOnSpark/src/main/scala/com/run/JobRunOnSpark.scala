package com.run

import com.boc.iff.exception._
import com.config.SparkJobConfig
import com.core.AppInit

/**
  *  Spark 程序入口
  *  @author www.birdiexx.com
  */
object JobRunOnSpark extends App{
  val config = new SparkJobConfig()
  val job = new AppInit[SparkJobConfig]()
  //val logger = job.logger
  try {
    job.start(config, args)
  } catch {
    case t:RecordNumberErrorException =>
      t.printStackTrace()
      System.exit(1)
    case t:MaxErrorNumberException =>
      t.printStackTrace()
      System.exit(2)
    case t:RecordNotFixedException =>
      t.printStackTrace()
      System.exit(3)
    case t:MaxBlankNumberException =>
      t.printStackTrace()
      System.exit(4)
    case t: Throwable =>
      t.printStackTrace()
      //if(StringUtils.isNotEmpty(t.getMessage)) logger.error("", t.getMessage)
      System.exit(9)
  }
}
