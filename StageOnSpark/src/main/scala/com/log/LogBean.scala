package com.log

/**
  * Created by tangzm on 2017/2/9.
  */
class LogBean {
  var hostName:String=""
  var jobID:String=""
  var threadID:String=""
  var msg:String=""
  def toLogMsg()={
    "HOSTNAME="+hostName+"|JOBID="+jobID+"|THREADID="+threadID+"|MESSAGE="+msg
  }
}
