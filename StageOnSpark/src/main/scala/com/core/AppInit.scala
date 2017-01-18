package com.core

import com.boc.iff._
import com.boc.iff.model._
import com.config.SparkJobConfig

/**
  * @author www.birdiexx.com
  */

class AppInit[T <: SparkJobConfig]  extends SparkJob[T]    {

  /**
   * 执行整个作业
   */
  protected def run(jobonfig: T): Unit = {
//    if(!prepare()) return
//
//    excutor()
//    logger.info(MESSAGE_ID_CNV1001, "File Conversion Complete! File: " + iffConversionConfig.iffFileInputPath)
  }



  /**
    * 注册使用 kryo 进行序列化的类
    *
    * @author www.birdiexx.com
    * @return
    **/
  override protected def kryoClasses: Array[Class[_]] = {
    Array[Class[_]](classOf[IFFMetadata], classOf[IFFSection], classOf[IFFField], classOf[IFFFileInfo],
      classOf[IFFFieldType], classOf[FormatSpec], classOf[ACFormat], classOf[StringAlign])
  }

  override protected def runOnSpark(jobonfig: T): Unit = {

  }

}

