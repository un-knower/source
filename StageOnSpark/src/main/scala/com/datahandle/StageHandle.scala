package com.datahandle

import com.boc.iff.DFSUtils
import com.boc.iff.exception.StageHandleException
import com.boc.iff.model.IFFFieldType
import com.context.{StageAppContext, StageRequest}
import com.log.LogBuilder
import com.model.{DebugInfo, TableInfo}
import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}
import org.apache.hadoop.io.IOUtils
import org.apache.spark.sql.{DataFrame, Row}

import scala.collection.mutable

/**
  * Created by cvinc on 2016/6/8.
  */
trait StageHandle[T<:StageRequest] {
    var appContext:StageAppContext = _
    var logBuilder:LogBuilder = _

    def doCommand(stRequest:StageRequest)(implicit  context:StageAppContext): Unit={
        appContext = context
        logBuilder = appContext.constructLogBuilder().setLogThreadID(Thread.currentThread().getId.toString)
        // before(stRequest)
        execute(stRequest)
       // after(stRequest)
    }

    protected def before(stRequest:StageRequest):Unit={
        if(stRequest.inputTables!=null&&stRequest.inputTables.size()>0){
            for(i<-0 until stRequest.inputTables.size() if(StringUtils.isNotBlank(stRequest.inputTables.get(i)))){
                val table = appContext.getTable(stRequest.inputTables.get(i))
                table.remainUsedTimes -= 1
            }
        }
    }

    protected def after(stRequest:StageRequest):Unit={
        if(stRequest.inputTables!=null&&stRequest.inputTables.size()>0){
            for(i<-0 until stRequest.inputTables.size() if(StringUtils.isNotBlank(stRequest.inputTables.get(i)))){
                val table = appContext.getTable(stRequest.inputTables.get(i))
                if(table.remainUsedTimes == 0)appContext.unCacheDataFrame(table.targetName)
            }
        }
    }



    def check(stRequest:StageRequest):Unit={
        if(stRequest.inputTables!=null&&(!stRequest.inputTables.isEmpty)){
            for(i<-0 until stRequest.inputTables.size()){
                if(!appContext.checkTableExist(stRequest.inputTables.get(i))){
                    throw new StageHandleException("Stage[%s]-Table[%s] not exists.".format(stRequest.stageId,i))
                }
            }
        }
    }

    def execute(stRequest:StageRequest): Unit

    protected def saveDebug(debugInfo:DebugInfo,df:DataFrame):Unit={
        implicit val config = appContext.sparkContext.hadoopConfiguration
        DFSUtils.deleteDir(debugInfo.file)
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
        if("LOG".equals(debugInfo.method)){

        }else {
            rdd.saveAsTextFile(debugInfo.file)
            combine(debugInfo)
        }
    }

    protected def combine(debugInfo:DebugInfo):Unit={
        val fileSystem = FileSystem.get(appContext.sparkContext.hadoopConfiguration)
        val sourceFilePath = new Path(debugInfo.file)
        if(fileSystem.exists(sourceFilePath)) {
            val target = "%s/%s".format(debugInfo.file, "TARGET")
            val targetFilePath = new Path(target)
            val out = fileSystem.create(targetFilePath)
            val fileStatus = fileSystem.getFileStatus(sourceFilePath)
            val fileStatusStrack: mutable.Stack[FileStatus] = new mutable.Stack[FileStatus]()
            fileStatusStrack.push(fileStatus)
            while (!fileStatusStrack.isEmpty) {
                val fst = fileStatusStrack.pop()
                if (fst.isDirectory) {
                    val fileStatusS = fileSystem.listStatus(fst.getPath)
                    for (f <- fileStatusS) {
                        fileStatusStrack.push(f)
                    }
                } else {
                    val in = fileSystem.open(fst.getPath)
                    IOUtils.copyBytes(in, out, 4096, false)
                    in.close(); //完成后，关闭当前文件输入流
                }
            }
            out.close();
            val files = fileSystem.listStatus(sourceFilePath)
            for (f <- files) {
                if (!f.getPath.toString.endsWith("TARGET")) {
                    fileSystem.delete(f.getPath, true)
                }
            }
        }
    }

    /**
      * 解析 元数据信息中的列数据格式定义
      *
      */
    protected def loadFieldTypeInfo(tableInfo:TableInfo): Unit = {
        if(tableInfo.body!=null&&tableInfo.body.fields!=null) {
            for (field <- tableInfo.body.fields.toArray) {
                if (field.typeInfo == null&&StringUtils.isNotEmpty(field.`type`)) {
                    field.typeInfo = IFFFieldType.getFieldType(tableInfo, null, field)
                }
            }
        }
    }


}
