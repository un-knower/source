package com.log

import java.net.{InetAddress, UnknownHostException}

import com.boc.iff.ECCLogger

import scala.beans.BeanProperty

/**
  * Created by cvinc on 2016/6/8.
  */

class LogBuilder(val logger:ECCLogger){
  val MESSAGE_ID_CNV1001 = "-CNV1001"
  val logBean:LogBean=new LogBean
  logBean.hostName = getHostName
  def setLogHostName(s:String)={
    logBean.hostName=s
    this
  }

  def setLogJobID(s:String)={
    logBean.jobID=s
    this
  }

  def setLogThreadID(s:String)={
    logBean.threadID=s
    this
  }

  def setLogMsg(s:String)={
    logBean.msg=s
    this
  }

  def build() ={
    logBean.toLogMsg()
  }

  def info(msg:String):Unit={
    logBean.msg=msg
    logger.info(MESSAGE_ID_CNV1001,logBean.toLogMsg())
  }

  def error(msg:String):Unit={
    logBean.msg=msg
    logger.error(MESSAGE_ID_CNV1001,logBean.toLogMsg())
  }

  def getHostName:String={
    try {
      return (InetAddress.getLocalHost()).getHostName();
    } catch {
      case e:UnknownHostException =>
        val host = e.getMessage(); // host = "hostname: hostname"
        if (host != null) {
          val colon = host.indexOf(':');
          if (colon > 0) {
            return host.substring(0, colon);
          }
        }
        return "UnknownHost";
    }
  }

}

object LogBuilder{


}
