package com.boc.iff.history

import com.boc.iff.DFSUtils
import com.boc.iff.IFFConversion._
import com.boc.iff.itf.DataProcessOnSparkConfig
import com.boc.iff.model._
import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.fs.{FileSystem, Path}

import scala.collection.mutable.ArrayBuffer

class F2FHistoryOnSparkJob  extends HistoryProcessOnSparkJob with Serializable {

}

/**
 * Spark 程序入口
 */
object F2FHistoryOnSpark extends App {
  val config = new DataProcessOnSparkConfig()
  val job = new F2FHistoryOnSparkJob()
  val logger = job.logger
  try {
    job.start(config, args)
  } catch {
    case t: Throwable =>
      t.printStackTrace()
      if (StringUtils.isNotEmpty(t.getMessage)) logger.error(MESSAGE_ID_CNV1001, t.getMessage)
      System.exit(1)
  }
}
