package com.log

import scala.beans.BeanProperty

/**
  * Created by cvinc on 2016/6/8.
  */

class LogBuilder{
  val logBean:LogBean=new LogBean
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

}

object LogBuilder{
  def main(args: Array[String]): Unit = {
    var log:LogBuilder = new LogBuilder
    var abc = log.setLogHostName("1").setLogJobID("2").setLogThreadID("3").setLogMsg("4").build()
    //abc= log.setLogMsg("efg").build()
    print(abc)
  }

}
