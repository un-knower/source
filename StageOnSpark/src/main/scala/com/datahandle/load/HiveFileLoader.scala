package com.datahandle.load

import java.io.FileInputStream
import java.util.Properties
import java.util.zip.GZIPInputStream

import com.boc.iff._
import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
  * Created by scutlxj on 2017/2/9.
  * 加载清洗后的文件
  */
class HiveFileLoader extends FileLoader{

  def loadFile(): DataFrame = {
    val targetRdd = this.sparkContext.textFile(this.fileInfo.dataPath)
    changeRddToDataFrame(targetRdd)
  }

}

class HiveFile2Loader extends SimpleFileLoader{

  override def loadFile(): DataFrame = {
    tableInfo.srcSeparator = this.fieldDelimiter
    if(StringUtils.isEmpty(tableInfo.sourceCharset)){
      tableInfo.sourceCharset = "UTF-8"
    }
    val convertByPartitions = createConvertByPartitionsFunction
    val conversionJob: ((Int, Long, Int, String) => RDD[Row]) = { (blockIndex, blockPosition, blockSize, filePath) =>
      val rdd = this.sparkContext.makeRDD(Seq((blockIndex, blockPosition, blockSize, filePath)), 1)
      rdd.mapPartitions(convertByPartitions)
    }
    createBlocks(conversionJob)
    var targetRdd = rddQueue.take()
    while(!rddQueue.isEmpty){
      targetRdd = targetRdd.union(rddQueue.take())
    }
    changeRddRowToDataFrame(targetRdd)
  }

  /**
    * 创建一个方法 对一个分片（分区）的数据进行转换操作
    *
    * @return
    */
  override protected def createConvertByPartitionsFunction: (Iterator[(Int, Long, Int,String)] => Iterator[Row]) = {
    // val iffConversionConfig = this.iffConversionConfig
    val tableInfo = this.tableInfo
    val iffFileInfo = this.iffFileInfo
    val lengthOfLineEnd = tableInfo.lengthOfLineEnd

    val fileGzipFlgMap = this.fileGzipFlgMap
    implicit val configuration = sparkContext.hadoopConfiguration
    val hadoopConfigurationMap = mutable.HashMap[String,String]()
    val iterator = configuration.iterator()
    while (iterator.hasNext){
      val entry = iterator.next()
      hadoopConfigurationMap += entry.getKey -> entry.getValue
    }
    val readBufferSize = this.readBufferSize

    val dataFileProcessor = this.getDataFileProcessor()
    dataFileProcessor.iffMetadata = tableInfo
    dataFileProcessor.lengthOfLineEnd = lengthOfLineEnd
    dataFileProcessor.iFFFileInfo = iffFileInfo

    val pro = new Properties
    pro.load(new FileInputStream(stageAppContext.jobConfig.configPath))



    val convertByPartitionsFunction: (Iterator[(Int, Long, Int,String)] => Iterator[Row]) = { blockPositionIterator =>
      val recordList = ListBuffer[Row]()
      val charset = IFFUtils.getCharset(tableInfo.sourceCharset)
      dataFileProcessor.charset = charset

      val logger = new ECCLogger()
      logger.configure(pro)

      val configuration = new YarnConfiguration()
      for((key,value)<-hadoopConfigurationMap){
        configuration.set(key, value)
      }

      while(blockPositionIterator.hasNext){
        val (blockIndex, blockPosition, blockSize,filePath) = blockPositionIterator.next()
        val fileSystem = FileSystem.get(configuration)
        val iffFileInputPath = new Path(filePath)
        val iffFileInputStream = fileSystem.open(iffFileInputPath)
        val iffFileSourceInputStream =
          if(fileGzipFlgMap(filePath)) new GZIPInputStream(iffFileInputStream, readBufferSize)
          else iffFileInputStream
        val inputStream = iffFileSourceInputStream.asInstanceOf[java.io.InputStream]
        var restToSkip = blockPosition
        while (restToSkip > 0) {
          val skipped = inputStream.skip(restToSkip)
          restToSkip = restToSkip - skipped
        }

        dataFileProcessor.open(inputStream,blockSize)
        while (dataFileProcessor.haveMoreLine()) {
          val fields = dataFileProcessor.getLineFields()
          recordList += Row.fromSeq(fields)
        }
        dataFileProcessor.close()
      }
      recordList.iterator
    }
    convertByPartitionsFunction
  }

}
