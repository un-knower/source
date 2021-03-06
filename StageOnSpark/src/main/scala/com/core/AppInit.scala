package com.core

import java.io.{File, FileInputStream}
import java.util.Properties

import com.boc.iff._
import com.boc.iff.exception.{StageHandleException, StageInfoErrorException}
import com.boc.iff.model._
import com.config.SparkJobConfig
import com.context.{StageAppContext, StageEngine}
import com.log.LogBuilder
import com.model.BatchInfo
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

  var jobConfig:T = _
  var appContext:StageAppContext = _
  var batchInfo:BatchInfo = _
  var logBuilder:LogBuilder = _
  /**
   * 执行整个作业
   */
  protected def run(jobConfig: T): Unit = {
    if(!prepare()) return
    val controller = new AppController
    controller.execute(appContext)
  }

  def prepare():Boolean = {
    appContext = new StageAppContext(sparkContext,jobConfig)
    logBuilder = appContext.constructLogBuilder()
    logBuilder.setLogThreadID(Thread.currentThread().getId.toString)
    loadMetadata(jobConfig.metadataFilePath,jobConfig.metadataFileEncoding)
    appContext.batchName = batchInfo.batchJobName
    appContext.stageEngine = new StageEngine(batchInfo.stages)
    if(batchInfo.stages!=null){
      appContext.stageEngine = new StageEngine(batchInfo.stages)
    }else{
      throw new StageInfoErrorException("No stage defined")
    }

    val batchArgNameSize = if(batchInfo.batchArgNames==null) 0 else batchInfo.batchArgNames.size()
    val batchArgSize = if(appContext.batchArgs==null) 0 else appContext.batchArgs.length
    if(batchArgNameSize!=batchArgSize){
      throw StageHandleException("Arg number not fix. This need [%s],provide arg number[%S]".format(batchArgNameSize,batchArgSize))
    }
    if(batchArgNameSize>0) {
      appContext.batchArgName = (for(i<-0 until batchArgNameSize) yield { batchInfo.batchArgNames.get(i)}).toArray
      val argMap = new StringBuffer()
      for(i<-0 until batchArgNameSize)argMap.append(appContext.batchArgName(i)).append("=").append(appContext.batchArgs(i)).append(" , ")
      logBuilder.info("Batch Job args[%s]".format(argMap.toString))
    }


    if(jobConfig.iffNumberOfThread>0) {
      logBuilder.info("Set thread numbers["+jobConfig.iffNumberOfThread+"]")
      System.setProperty("scala.concurrent.context.minThreads", String.valueOf(jobConfig.iffNumberOfThread))
      System.setProperty("scala.concurrent.context.numThreads", String.valueOf(jobConfig.iffNumberOfThread))
      System.setProperty("scala.concurrent.context.maxThreads", String.valueOf(jobConfig.iffNumberOfThread))
    }
    println("************************ version time 2017-03-14 16:54 ***************************")
    true
  }



  /**
    * 解析 XML 元数据文件
    *
    * @param metadataFileName XML 元数据文件路径
    * @param encoding         XML 元数据文件编码
    */
  protected def loadMetadata(metadataFileName: String, encoding: String): Unit = {
    logBuilder.info("loadMetadata "+ metadataFileName)
    val metadataFile = new File(metadataFileName)
    var metadataXml = FileUtils.readFileToString(metadataFile, encoding)
    metadataXml = StringUtils.replace(metadataXml, "com.boc.oms.model.", "com.boc.iff.model.")
    metadataXml = StringUtils.replace(metadataXml, "com.boc.isb.util.iff.", "com.boc.iff.")
    var metadataXmlResource = new ByteArrayResource(metadataXml.getBytes(encoding))
    var appContext = new GenericXmlApplicationContext()
    appContext.setValidating(false)
    appContext.load(metadataXmlResource)
    appContext.refresh()
    try {
      val batchClass = classOf[BatchInfo]
      batchInfo = appContext.getBean("batchInfo", batchClass)
      for(i<- 0 until batchInfo.batchArgNames.size() ){
        val argString = "#"+batchInfo.batchArgNames.get(i)+"#"
        metadataXml = StringUtils.replace(metadataXml,argString,jobConfig.batchArgs(i))
      }

      metadataXmlResource = new ByteArrayResource(metadataXml.getBytes(encoding))
      appContext = new GenericXmlApplicationContext()
      appContext.setValidating(false)
      appContext.load(metadataXmlResource)
      appContext.refresh()
      batchInfo = appContext.getBean("batchInfo", batchClass)

      logBuilder.info("loadMetadata "+ metadataFileName+" Success")
    }
    catch {
      case e: BeansException =>
        logBuilder.error(metadataFileName + " BeansException.")
        throw e
      case e: ClassNotFoundException =>
        logBuilder.error(metadataFileName + " ClassNotFoundException.")
        throw e
    }
  }

  protected def replaceArgs(sql:String):String={
    var processSql = sql
    if(appContext.batchArgName!=null&&appContext.batchArgName.length>0){
      for(i<- 0 until appContext.batchArgName.length ){
        val argString = "#"+appContext.batchArgName(i)+"#"
        processSql = StringUtils.replace(processSql,argString,appContext.batchArgs(i))
      }
    }
    processSql
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

  override protected def runOnSpark(jobConfig: T): Unit = {
    this.jobConfig = jobConfig
    run(jobConfig)
  }

}

