package com.core

import java.io.File

import com.boc.iff._
import com.boc.iff.model._
import com.config.SparkJobConfig
import org.apache.commons.io.FileUtils
import org.apache.commons.lang3.StringUtils
import org.springframework.beans.BeansException
import org.springframework.beans.factory.NoSuchBeanDefinitionException
import org.springframework.context.support.GenericXmlApplicationContext
import org.springframework.core.io.ByteArrayResource

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
    * 解析 XML 元数据文件
    *
    * @param metadataFileName XML 元数据文件路径
    * @param encoding         XML 元数据文件编码
    */
  protected def loadMetadata(metadataFileName: String, encoding: String): Unit = {
    val metadataFile = new File(metadataFileName)
    var metadataXml = FileUtils.readFileToString(metadataFile, encoding)
    metadataXml = StringUtils.replace(metadataXml, "com.boc.oms.model.", "com.boc.iff.model.")
    metadataXml = StringUtils.replace(metadataXml, "com.boc.isb.util.iff.", "com.boc.iff.")
    val metadataXmlResource = new ByteArrayResource(metadataXml.getBytes(encoding))
    val appContext = new GenericXmlApplicationContext()
    appContext.setValidating(false)
    appContext.load(metadataXmlResource)
    appContext.refresh()

    try {
      val iffSectionClass = classOf[IFFSection]
      iffMetadata.header = appContext.getBean("header", iffSectionClass)
      iffMetadata.body = appContext.getBean("body", iffSectionClass)
      iffMetadata.footer = appContext.getBean("footer", iffSectionClass)
    }
    catch {
      case e: BeansException =>
        logger.error(MESSAGE_ID_CNV1001, iffConversionConfig.metadataFilePath + " BeansException.")
        throw e
      case e: ClassNotFoundException =>
        logger.error(MESSAGE_ID_CNV1001, iffConversionConfig.metadataFilePath + " ClassNotFoundException.")
        throw e
    }
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

