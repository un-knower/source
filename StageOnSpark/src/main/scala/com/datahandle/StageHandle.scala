package com.datahandle

import com.context.{StageAppContext, StageRequest}
import com.log.LogBuilder
import com.model.DebugInfo
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row}

/**
  * Created by cvinc on 2016/6/8.
  */
trait StageHandle[T<:StageRequest] {
    var appContext:StageAppContext = _
    var logBuilder:LogBuilder = _

    def execute(stRequest:StageRequest)(implicit  context:StageAppContext): Unit={
        appContext = context
        logBuilder = appContext.constructLogBuilder().setLogThreadID(Thread.currentThread().getId.toString)
        doCommand(stRequest)
    }
    def doCommand(stRequest:StageRequest): Unit

    protected def saveDebug(debugInfo:DebugInfo,df:DataFrame):Unit={
        val newDF = df.limit(debugInfo.limit)
        val targetSeparator = debugInfo.delimiter
        val rowToString = (x: Row) => {
            val rowData = x.toSeq
            val str = new StringBuffer()
            var index = 0
            for (v <- rowData) {
                if(index>0){
                    str.append(targetSeparator)
                }
                str.append(v)
                index+=1
            }
            str.toString
        }
        val rdd = newDF.rdd.map(rowToString)
        rdd.saveAsTextFile(debugInfo.file)
    }

    protected def saveDebug(debugInfo:DebugInfo,rdd:RDD):Unit={
        rdd.saveAsTextFile(debugInfo.file)
    }

    protected def combin(debugInfo:DebugInfo,rdd:RDD):Unit={
        rdd.saveAsTextFile(debugInfo.file)
    }


}
