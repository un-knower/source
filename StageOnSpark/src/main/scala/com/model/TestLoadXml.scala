package com.model

import java.io.File

import org.apache.commons.io.FileUtils
import org.apache.commons.lang3.StringUtils
import org.springframework.beans.BeansException
import org.springframework.context.support.GenericXmlApplicationContext
import org.springframework.core.io.ByteArrayResource

/**
  * Created by scutlxj on 2017/2/9.
  */
object TestLoadXml {

  def main(args: Array[String]) {
    loadMetadata("g:\\sparkletStageModel.xml","UTF-8")
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
      val batchClass = classOf[BatchInfo]
      val batchInfo = appContext.getBean("batchInfo", batchClass)
      println(batchInfo.stages.size())
    }
    catch {
      case e: BeansException =>
        //logger.error(MESSAGE_ID_CNV1001, metadataFileName + " BeansException.")
        throw e
      case e: ClassNotFoundException =>
        //logger.error(MESSAGE_ID_CNV1001, metadataFileName + " ClassNotFoundException.")
        throw e
    }
  }

}
