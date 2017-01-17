package com.boc.iff.load

import java.io._
import java.util
import java.util.Properties
import java.util.concurrent.LinkedBlockingQueue
import java.util.zip.GZIPInputStream

import com.boc.iff.DFSUtils.FileMode
import com.boc.iff._
import com.boc.iff.IFFConversion._
import com.boc.iff.exception._
import com.boc.iff.model._
import org.apache.commons.io.FileUtils
import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path, PathFilter}
import org.apache.hadoop.io.IOUtils
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext, SaveMode}
import org.apache.spark.sql.types.{DataTypes, StructField}

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent._
import scala.concurrent.duration.Duration
import scala.util.{Failure, Success}

/**
  * @author www.birdiexx.com
  */
class BaseConversionOnSparkConfig extends IFFConversionConfig with SparkJobConfig {

  var iffFileMode: DFSUtils.FileMode.ValueType = DFSUtils.FileMode.LOCAL
  var maxBlockSize: Int = -1                                                //最大每次读取文件的块大小
  var minBlockSize: Int = -1                                                //最小每次读取文件的块大小
  var dataLineEndWithSeparatorF:String = "Y"                            //数据文件行是否以分隔符结束 Y-是 N-否
  var maxCreateBlockThreadUum:Int = 1                            //分块最大线程数
  var dynamicFileAccept:Boolean = false                          //动态接受文件

  override protected def makeOptions(optionParser: scopt.OptionParser[_]) = {
    super.makeOptions(optionParser)
    optionParser.opt[String]("iff-file-mode")
      .text("IFF File Mode")
      .foreach{ x=> this.iffFileMode = DFSUtils.FileMode.withName(x) }
    optionParser.opt[String]("max-block-size")
      .text("Max Block Size")
      .foreach { x=>this.maxBlockSize = IFFUtils.getSize(x) }
    optionParser.opt[String]("min-block-size")
      .text("Min Block Size")
      .foreach { x=>this.minBlockSize = IFFUtils.getSize(x) }
    optionParser.opt[String]("data-line-end-with-separator-f")
      .text("dataLineEndWithSeparatorF")
      .foreach { x=>this.dataLineEndWithSeparatorF = x }
    optionParser.opt[Int]("max_create_block_thread_num")
      .text("maxCreateBlockThreadUum")
      .foreach { x=>this.maxCreateBlockThreadUum = x }
    optionParser.opt[String]("dynamic-file-accept")
      .text("dynamicFileAccept")
      .foreach { x=>if("Y".equals(x))dynamicFileAccept = true }
  }

  override def toString = {
    val builder = new mutable.StringBuilder(super.toString)
    if(builder.nonEmpty) builder ++= "\n"
    builder ++= "IFF File Mode: %s\n".format(iffFileMode.toString)
    builder ++= "Max Block Size: %d\n".format(maxBlockSize)
    builder ++= "Min Block Size: %d".format(minBlockSize)
    builder.toString
  }
}


/**
  * @author www.birdiexx.com
  */
trait BaseConversionOnSparkJob[T<:BaseConversionOnSparkConfig]
  extends IFFConversion[T] with SparkJob[T]  {
  val fileGzipFlgMap = new mutable.HashMap[String,Boolean]

  val fileInMap = new mutable.HashMap[String,String]
  protected val standbyFileQueue = new LinkedBlockingQueue[String]()
  protected val processFileQueue = new LinkedBlockingQueue[String]()
  protected val futureQueue = new LinkedBlockingQueue[Future[String]]()
  protected var allFileAccepted:Boolean = false



  protected def getTempDir: String = {
    iffConversionConfig.tempDir + "/" + StringUtils.split(iffConversionConfig.iffFileInputPath, "/").last
  }

  protected def getErrorFileDir: String = {
    val filePath = iffConversionConfig.errorFilDir + "/" + StringUtils.split(iffConversionConfig.iffFileInputPath, "/").last
    filePath
  }

  protected def createDir(path:String): Unit ={
    iffConversionConfig.iffFileMode match {
      case FileMode.LOCAL =>
        var f = new File(path)
        val fileStack = new mutable.Stack[File]()
        if (!f.exists()) {
          fileStack.push(f)
          while (!f.getParentFile.exists()) {
            f = f.getParentFile
            fileStack.push(f)
          }
        }
        while (!fileStack.isEmpty) {
          f = fileStack.pop()
          f.mkdir()
        }
      case _ =>
        implicit val configuration = sparkContext.hadoopConfiguration
        DFSUtils.createDir(path)
    }
  }

  /**
    * 检查DFS上文件路径是否存在
    *
    * @param fileName 文件路径
    * @return
    */
  protected def checkDFSFileExists(fileName: String): Boolean = {
    val fileSystem = FileSystem.get(sparkContext.hadoopConfiguration)
    val path = new Path(fileName)
    if(!fileSystem.exists(path)) {
      logger.error(MESSAGE_ID_CNV1001, "File 	:" + path.toString + " not exists.")
      false
    }else true
  }

  /**
    * 检查文件路径是否存在
    *
    * @param fileName 文件路径
    * @return
    */
  protected def checkFileExists(fileName: String,
                                fileMode: FileMode.ValueType = FileMode.LOCAL): Boolean = {
    fileMode match {
      case FileMode.LOCAL => checkLocalFileExists(fileName)
      case FileMode.DFS => checkDFSFileExists(fileName)
    }
  }

  /**
    * 检查参数中定义的 配置文件、XML 元数据文件和 IFF 文件是否存在
    *
    * @return
    */
  override protected def checkFilesExists: Boolean = {
    if(!checkFileExists(iffConversionConfig.configFilePath)||
      !checkFileExists(iffConversionConfig.metadataFilePath)||
      !checkFileExists(iffConversionConfig.iffFileInputPath, iffConversionConfig.iffFileMode)) {
      false
    } else {
      true
    }
  }


  override protected def getIFFFileLength(fileName: String, isGZip: Boolean): Long = {
    iffConversionConfig.iffFileMode match {
      case FileMode.LOCAL => super.getIFFFileLength(fileName, isGZip)
      case FileMode.DFS =>
        val fileSystem = FileSystem.get(sparkContext.hadoopConfiguration)
        val filePath = new Path(fileName)
        val fileStatus = fileSystem.getFileStatus(filePath)
        if(isGZip) fileStatus.getLen * 10
        else fileStatus.getLen
    }
  }

  override protected def loadIFFFileInfo(iffFileName: String, readBufferSize: Int): Unit ={
    if(isDirectory(iffFileName)){
      iffFileInfo = new IFFFileInfo()
      iffFileInfo.gzip = false
      val sourceCharsetName =
        if(StringUtils.isNotEmpty(iffMetadata.sourceCharset)) {
          iffMetadata.sourceCharset
        }else{
          "UTF-8"
        }
      iffMetadata.sourceCharset = sourceCharsetName
      logger.info(MESSAGE_ID_CNV1001, "SrcCharset: " + sourceCharsetName)
      iffFileInfo.recordLength = iffMetadata.header.getLength
      iffFileInfo.recordLength = math.max(iffFileInfo.recordLength, iffMetadata.body.getLength)
      iffFileInfo.recordLength = math.max(iffFileInfo.recordLength, iffMetadata.footer.getLength)
      if(iffMetadata.fixedLength!=null) {
        iffFileInfo.recordLength = math.max(iffFileInfo.recordLength, iffMetadata.fixedLength.toInt)
      }
      logger.info(MESSAGE_ID_CNV1001, "iffFile REC_LENGTH = " + iffFileInfo.recordLength)
      iffFileInfo.dir = true
    }else{
      super.loadIFFFileInfo(iffFileName,readBufferSize)
    }

  }

  protected def isDirectory(fileName: String): Boolean = {
    iffConversionConfig.iffFileMode match {
      case FileMode.LOCAL =>
        val f = new File(fileName)
        f.isDirectory()
      case FileMode.DFS =>
        val fileSystem = FileSystem.get(sparkContext.hadoopConfiguration)
        val filePath = new Path(fileName)
        fileSystem.isDirectory(filePath)
    }
  }

  override protected def openIFFFileInputStream(fileName: String): java.io.InputStream = {
    iffConversionConfig.iffFileMode match {
      case FileMode.LOCAL => super.openIFFFileInputStream(fileName)
      case FileMode.DFS =>
        val fileSystem = FileSystem.get(sparkContext.hadoopConfiguration)
        val filePath = new Path(fileName)
        val iffFileInputStream = fileSystem.open(filePath, iffConversionConfig.readBufferSize)
        iffFileInputStream
    }
  }

  protected def getIFFFilePath(fileName: String):Unit = {
    iffConversionConfig.iffFileMode match {
      case FileMode.LOCAL =>
        val f = new File(fileName)
        if(f.isDirectory){
          for(i<-f.listFiles() if !i.isDirectory){
            standbyFileQueue.put(i.getPath)
            fileGzipFlgMap += (fileName->checkGZipFile(fileName))
          }
        }else{
          standbyFileQueue.put(fileName)
          fileGzipFlgMap += (fileName->checkGZipFile(fileName))
        }
        allFileAccepted=true
      case FileMode.DFS =>
        val fileSystem = FileSystem.get(sparkContext.hadoopConfiguration)
        val filePath = new Path(fileName)
        if(fileSystem.isDirectory(filePath)){
          //logger.info(MESSAGE_ID_CNV1001,"scan filePath: "+fileName)
          val completeFile = new PathFilter {
            override def accept(path: Path): Boolean = {
              !path.getName.toLowerCase().endsWith("_copying_")&&(!fileInMap.contains(path.toString))
            }
          }
          val fileStatus = fileSystem.listStatus(filePath, completeFile)
          var acceptNewFileNmu = 0
          for(fs<-fileStatus) {
            //logger.info("getPath****************", "**********" + fs.getPath.toString)
            if(fs.getPath.getName.toLowerCase.equals("end")){
              allFileAccepted=true
            }else {
              fileInMap += (fs.getPath.toString -> fs.getPath.toString)
              fileGzipFlgMap += (fs.getPath.toString -> checkGZipFile(fs.getPath.toString))
              standbyFileQueue.put(fs.getPath.toString)
              acceptNewFileNmu+=1
            }
          }
          if(acceptNewFileNmu>0)logger.info(MESSAGE_ID_CNV1001,"Accept["+acceptNewFileNmu+"] new file")
          if(!iffConversionConfig.dynamicFileAccept){
            allFileAccepted=true
          }
          if(allFileAccepted)logger.info(MESSAGE_ID_CNV1001,"*********************** All file Accepted ****************************")
        }else{
          standbyFileQueue.put(fileName)
          fileGzipFlgMap += (fileName->checkGZipFile(fileName))
          allFileAccepted=true
        }
    }
  }

  protected def createBlocks(conversionJob: ((Int, Long, Int,String)=>String)): Unit = {
    while(!allFileAccepted){
      getIFFFilePath(iffConversionConfig.iffFileInputPath+iffConversionConfig.filename)
      if(this.standbyFileQueue.size()==0){
        Thread.sleep(10000)
      }else{
        while(this.standbyFileQueue.size()>0) {
          if (this.processFileQueue.size() >= iffConversionConfig.maxCreateBlockThreadUum) {
            Thread.sleep(5000)
          } else {
            val filePath = standbyFileQueue.take()
            logger.info(MESSAGE_ID_CNV1001,"createBlock [ "+filePath+" ]")
            this.processFileQueue.put(filePath)
            val fu = future {
              createBlockPositionQueue(filePath, conversionJob)
            }
            fu onComplete {
              case Success(x) => this.processFileQueue.take()
              case Failure(e) =>
                throw e
            }
            futureQueue.put(fu)
          }
        }
      }
    }
    while(!futureQueue.isEmpty){
      val future = futureQueue.take()
      Await.ready(future, Duration.Inf)
    }
  }

  protected def getDataFileProcessor():DataFileProcessor

  protected def createBlockPositionQueue(filePath:String,conversionJob: ((Int, Long, Int,String)=>String)):String

  /**
    * 创建一个方法 对一个分片（分区）的数据进行转换操作
    *
    * @return
    */
  protected def createConvertByPartitionsFunction: (Iterator[(Int, Long, Int,String)] => Iterator[String]) = {
    val iffConversionConfig = this.iffConversionConfig
    val lengthOfLineEnd = iffConversionConfig.lengthOfLineEnd
    val iffMetadata = this.iffMetadata
    val iffFileInfo = this.iffFileInfo
    val fieldDelimiter = this.fieldDelimiter
    val lineSplit = iffMetadata.srcSeparator
    val fileGzipFlgMap = this.fileGzipFlgMap
    val specialCharConvertor = this.specialCharConvertor
    val runOnDFS = iffConversionConfig.iffFileMode match {
      case DFSUtils.FileMode.LOCAL => false
      case _ => true
    }
    val needConvertSpecialChar:Boolean = if("Y".equals(this.iffConversionConfig.specialCharConvertFlag))true else false
    implicit val configuration = sparkContext.hadoopConfiguration
    val hadoopConfigurationMap = mutable.HashMap[String,String]()
    val iterator = configuration.iterator()
    val prop = new Properties()
    prop.load(new FileInputStream(iffConversionConfig.configFilePath))
    while (iterator.hasNext){
      val entry = iterator.next()
      hadoopConfigurationMap += entry.getKey -> entry.getValue
    }
    val readBufferSize = iffConversionConfig.readBufferSize

    val dataFileProcessor = this.getDataFileProcessor()
    dataFileProcessor.iffMetadata = iffMetadata
    dataFileProcessor.lengthOfLineEnd = lengthOfLineEnd
    dataFileProcessor.needConvertSpecialChar = needConvertSpecialChar
    dataFileProcessor.specialCharConvertor = specialCharConvertor
    dataFileProcessor.iffFileInfo = iffFileInfo



    val convertByPartitionsFunction: (Iterator[(Int, Long, Int,String)] => Iterator[String]) = { blockPositionIterator =>
      val logger = new ECCLogger()
      logger.configure(prop)
      val recordList = ListBuffer[String]()


      val charset = IFFUtils.getCharset(iffMetadata.sourceCharset)
      dataFileProcessor.charset = charset
      val decoder = charset.newDecoder

      /*
        对一个字段的数据进行转换操作
        为了减少层次，提高程序可读性，这里定义了一个闭包方法作为参数，会在下面的 while 循环中被调用
       */
      val convertField: (IFFField, java.util.HashMap[String,Any])=> String = { (iffField, record) =>
        if (iffField.isFilter) ""
        else if (iffField.isConstant) {
          iffField.getDefaultValue.replaceAll("#FILENAME#", iffFileInfo.fileName)
        } else {
          import com.boc.iff.CommonFieldConvertorContext._
          implicit val convertorContext = new CommonFieldConvertorContext(iffMetadata, iffFileInfo, decoder)
          try {
            /*
              通过一些隐式转换对象和方法，使 IFFField 对象看起来像拥有了 convert 方法一样
              化被动为主动，可使程序语义逻辑更清晰
             */
            iffField.convert(record)
          } catch {
            case e: Exception =>
              logger.error(MESSAGE_ID_CNV1001, "invaild record found in : " + iffField.getName)
              logger.error(MESSAGE_ID_CNV1001, "invaild record found data : " + record)
              ""
          }
        }
      }

      val configuration = new YarnConfiguration()
      for((key,value)<-hadoopConfigurationMap){
        configuration.set(key, value)
      }

      while(blockPositionIterator.hasNext){
        val (blockIndex, blockPosition, blockSize,filePath) = blockPositionIterator.next()
        val iffFileInputStream = if(runOnDFS){
          val fileSystem = FileSystem.get(configuration)
          val iffFileInputPath = new Path(filePath)
          fileSystem.open(iffFileInputPath)
        }else{
          new FileInputStream(new File(filePath))
        }
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
          if (fields.size>0) {
            var dataInd = 0
            val dataMap = new util.HashMap[String,Any]()
            val sb = new StringBuffer()
            var success = true
            var errorMessage = ""
            var currentName = ""
            var currentValue = ""
            try {
              for (iffField <- iffMetadata.body.fields if (!iffField.virtual)) {
                val fieldType = iffField.typeInfo
                currentName = iffField.name
                if(dataInd>=fields.length){
                  currentValue=""
                }else {
                  currentValue = fields(dataInd)
                }
                if(StringUtils.isNotBlank(currentValue)) {
                  fieldType match {
                    case fieldType: CInteger => dataMap.put(iffField.name ,currentValue.trim.toInt)
                    case fieldType: CDecimal => dataMap.put(iffField.name , currentValue.trim.toDouble)
                    case _ => dataMap.put(iffField.name , currentValue.trim)
                  }
                }else{
                  dataMap.put(iffField.name , "")
                }
                dataInd += 1
              }
            } catch {
              case e: NumberFormatException =>
                success = false
                errorMessage = " " + currentName+"["+currentValue+"] String to Number Exception "
              case e: Exception =>
                success = false
                errorMessage = " " +currentName+"["+currentValue+"] unknown exception " + e.getMessage
            }
            if (success) {
              errorMessage = ""
              implicit val validContext = new CommonFieldValidatorContext
              import com.boc.iff.CommonFieldValidatorContext._
              for (iffField <- iffMetadata.body.fields if success && !iffField.isFilter) {
                try {
                  success = if (iffField.validateField(dataMap)) true else false
                  if(success){
                    sb.append(convertField(iffField, dataMap)).append(fieldDelimiter) //`
                  }else{
                    errorMessage =  iffField.name + " ERROR validateField"
                  }
                }catch{
                  case e:Exception=>
                    success = false
                    errorMessage = iffField.name+" "+e.getMessage
                }
              }
            }
            if (!success) {
              sb.setLength(0)
              sb.append(fields.reduceLeft(_+iffMetadata.srcSeparator+_)).append(lineSplit).append(errorMessage).append(" ERROR")
            }
            recordList += sb.toString
          }
        }
        dataFileProcessor.close()
      }
      recordList.iterator
    }
    convertByPartitionsFunction
  }

  protected def convertFileOnSpark(): Unit = {
    val iffConversionConfig = this.iffConversionConfig
    val convertByPartitions = createConvertByPartitionsFunction
    val broadcast: org.apache.spark.broadcast.Broadcast[mutable.ListBuffer[Long]]
    = sparkContext.broadcast(mutable.ListBuffer())
    val tempDir = getTempDir
    val errorDir = getErrorFileDir
    deleteDir(tempDir)
    deleteDir(errorDir)
    createDir(tempDir)
    createDir(errorDir)
    val conversionJob: ((Int, Long, Int,String)=>String) = { (blockIndex, blockPosition, blockSize, filePath)=>
      val filename = filePath.substring(filePath.lastIndexOf("/")+1)
      val rdd = sparkContext.makeRDD(Seq((blockIndex, blockPosition, blockSize, filePath)), 1)
      val convertedRecords = rdd.mapPartitions(convertByPartitions)
      val tempOutputDir = "%s/%s-%05d".format(tempDir, filename,blockIndex)
      val errorOutputDir = "%s/%s-%05d".format(errorDir, filename,blockIndex)
      convertedRecords.cache()
      val errorRcdRDD = convertedRecords.filter(_.endsWith("ERROR"))
      if(iffConversionConfig.fileMaxError>0) {
        val errorRecordNumber = errorRcdRDD.count()
        broadcast.value += errorRecordNumber
      }
      saveRdd(convertedRecords.filter(!_.endsWith("ERROR")),tempOutputDir)
      saveRdd(errorRcdRDD,errorOutputDir)
      convertedRecords.unpersist()
      tempOutputDir
    }
    createBlocks(conversionJob)
    //combineMutilFileToSingleFile(errorDir)
    if(iffConversionConfig.fileMaxError>0) {
      val errorRec = broadcast.value.foldLeft(0L)(_ + _)
      logger.info("errorRec", "errorRec:" + errorRec)
      if (errorRec >= iffConversionConfig.fileMaxError) {
        throw MaxErrorNumberException("errorRec:" + errorRec + "iffConversionConfig.fileMaxError" + iffConversionConfig.fileMaxError)
      }
    }
    saveTargetData()
    deleteDir(tempDir)
  }

  protected def deleteDir(path:String):Unit={
    iffConversionConfig.iffFileMode match{
      case DFSUtils.FileMode.LOCAL=>
        val file = new File(path)
        if(file.exists()){
          val fileStatusStrack: mutable.Stack[File] = new mutable.Stack[File]()
          val rmFileStatusStrack: mutable.Stack[File] = new mutable.Stack[File]()
          fileStatusStrack.push(file)
          while (!fileStatusStrack.isEmpty) {
            val fst = fileStatusStrack.pop()
            rmFileStatusStrack.push(fst)
            if (fst.isDirectory) {
              val fileStatusS = fst.listFiles()
              for (f <- fileStatusS) {
                fileStatusStrack.push(f)
              }
            }
          }
          while (!rmFileStatusStrack.isEmpty) {
            val fst = rmFileStatusStrack.pop()
            fst.delete()
          }
        }
      case _ =>
        implicit val configuration = sparkContext.hadoopConfiguration
        DFSUtils.deleteDir(path)
    }
  }

  protected def saveRdd(rdd:RDD[String],targetPath:String): Unit ={
    println("saveRDD to "+targetPath)
    iffConversionConfig.iffFileMode match {
      case FileMode.LOCAL=>
        val result = rdd.collect()
        println("**********************size:"+result.length)
        var bw:BufferedWriter = null
        var os:OutputStream = null
        try{
          os = new FileOutputStream(targetPath)
          bw = new BufferedWriter(new OutputStreamWriter(os,"UTF-8"))
          var writeSzie = 0
          for(line<-result){
            bw.write(line+"\n")
            writeSzie+=1
          }
          println("**********************writeSzie:"+writeSzie)
        }catch{
          case e:Throwable =>
            try{
              if(bw!=null)bw.close()
            }catch {
              case e:Throwable =>
            }
            try{
              if(os!=null)os.close()
            }catch {
              case  e:Throwable =>
            }
        }
      case _ => rdd.saveAsTextFile(targetPath)
    }

  }

  protected def combineMutilFileToSingleFile(path:String):Unit={

    iffConversionConfig.iffFileMode match {
      case FileMode.LOCAL =>
        val fl = new File(path)
        if(fl.exists()) {
          val out = new FileOutputStream(new File("%s/%s".format(path, "TARGET")))
          val file = new File(path)
          val fileStatusStrack: mutable.Stack[File] = new mutable.Stack[File]()
          val rmFileStatusStrack: mutable.Stack[File] = new mutable.Stack[File]()
          fileStatusStrack.push(file)
          val files = file.listFiles(new FileFilter {
            override def accept(pathname: File): Boolean = (!pathname.getName.equals("TARGET"))
          })
          for (f <- files ) {
            fileStatusStrack.push(f)
          }
          while (!fileStatusStrack.isEmpty) {
            val fst = fileStatusStrack.pop()
            rmFileStatusStrack.push(fst)
            if (fst.isDirectory) {
              val fileStatusS = fst.listFiles()
              for (f <- fileStatusS ) {
                fileStatusStrack.push(f)
              }
            } else if(fst.length()>0&&(!fst.getName.endsWith(".crc"))){
              logger.info(MESSAGE_ID_CNV1001,"copy"+fst.getPath)
              FileUtils.copyFile(fst, out)
            }
          }
          out.close()
          while (!rmFileStatusStrack.isEmpty) {
            val fst = rmFileStatusStrack.pop()
            fst.delete
          }
        }
      case _ =>
        val fileSystem = FileSystem.get(sparkContext.hadoopConfiguration)
        val sourceFilePath = new Path(path)
        if(fileSystem.exists(sourceFilePath)) {
          val target = "%s/%s".format(path, "TARGET")
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

  }

  protected def saveTargetData():Unit={
    if(iffConversionConfig.autoDeleteTargetDir)cleanDataFile()
    iffConversionConfig.iffFileMode match {
      case FileMode.LOCAL=>
        val fileStatusStrack:mutable.Stack[File] = new mutable.Stack[File]()
        fileStatusStrack.push(new File(getTempDir))
        var index:Int = 0;
        while(!fileStatusStrack.isEmpty){
          val fst = fileStatusStrack.pop()
          if(fst.isDirectory){
            val fileStatusS = fst.listFiles()
            for(f<-fileStatusS){
              fileStatusStrack.push(f)
            }
          }else if(fst.length()>0&&(!fst.getName.endsWith(".crc"))){
            val fileName =  "%s/%s-%03d".format(iffConversionConfig.datFileOutputPath,sparkContext.applicationId, index)
            //FileUtils.moveFile(fst,new File(fileName))
            if(fst.renameTo(new File(fileName))){
              logger.info(MESSAGE_ID_CNV1001,"Move File:"+fst.getPath+" to "+fileName)
            }else{
              logger.info(MESSAGE_ID_CNV1001,"Move File:"+fst.getPath+" fail ")
            }
            index+=1
          }
        }
      case _ =>
        val fileSystem = FileSystem.get(sparkContext.hadoopConfiguration)
        val sourceFilePath = new Path(getTempDir)
        val fileStatus = fileSystem.getFileStatus(sourceFilePath)
        val fileStatusStrack:mutable.Stack[FileStatus] = new mutable.Stack[FileStatus]()
        fileStatusStrack.push(fileStatus)
        var index:Int = 0;
        while(!fileStatusStrack.isEmpty){
          val fst = fileStatusStrack.pop()
          if(fst.isDirectory){
            val fileStatusS = fileSystem.listStatus(fst.getPath)
            for(f<-fileStatusS){
              fileStatusStrack.push(f)
            }
          }else if(fst.getLen>0){
            val fileName =  "%s/%s-%03d".format(iffConversionConfig.datFileOutputPath,sparkContext.applicationId, index)
            val srcPath = fst.getPath
            val dstPath = new Path(fileName)
            DFSUtils.moveFile(fileSystem,srcPath, dstPath)
            index+=1
          }
        }
    }

  }

  protected def cleanDataFile():Unit={
    logger.info("MESSAGE_ID_CNV1001","****************clean target dir ********************")
    deleteDir(iffConversionConfig.datFileOutputPath)
    iffConversionConfig.iffFileMode match {
      case DFSUtils.FileMode.LOCAL =>
        val file = new File(iffConversionConfig.datFileOutputPath)
        if(!file.exists()||(!file.isDirectory)){
          createDir(iffConversionConfig.datFileOutputPath)
        }
      case _ =>
        val fileSystem = FileSystem.get(sparkContext.hadoopConfiguration)
        val datFileOutputPath = new Path(iffConversionConfig.datFileOutputPath)
        if (!fileSystem.isDirectory(datFileOutputPath)){
          logger.info(MESSAGE_ID_CNV1001, "Create Dir: " + datFileOutputPath.toString)
          fileSystem.mkdirs(datFileOutputPath)
        }
    }

  }

  /**
    * 转换 IFF 数据文件
    *
    * @author www.birdiexx.com
    */
  override protected def convertFile(): Unit = {
    convertFileOnSpark()
  }

  /**
    * 准备阶段：
    * 1. 检查输入文件是否存在
    * 2. 加载配置文件
    * 3. 查询目标表信息
    *
    * @author www.birdiexx.com
    * @return
    */
  override protected def prepare(): Boolean = {
    val result = super.prepare()
    if(iffConversionConfig.maxBlockSize > 0 && iffConversionConfig.minBlockSize > 0){
      if(!iffFileInfo.dir) {
        val fitBlockSize = (iffFileInfo.fileLength / (if (maxExecutors == 0) 1 else maxExecutors)) + (iffFileInfo.fileLength % (if (maxExecutors == 0) 1 else maxExecutors))
        iffConversionConfig.blockSize =
          math.min(iffConversionConfig.maxBlockSize, math.max(iffConversionConfig.minBlockSize, fitBlockSize)).toInt
      }else{
        iffConversionConfig.blockSize = iffConversionConfig.maxBlockSize
      }
    }
    numberOfThread = if(this.iffConversionConfig.iffNumberOfThread>0){
      this.iffConversionConfig.iffNumberOfThread
    }else if(dynamicAllocation) {
      maxExecutors
    }else{
      numExecutors
    }
    System.setProperty("scala.concurrent.context.minThreads", String.valueOf(numberOfThread))
    System.setProperty("scala.concurrent.context.numThreads", String.valueOf(numberOfThread))
    System.setProperty("scala.concurrent.context.maxThreads", String.valueOf(numberOfThread))
    logger.info(MESSAGE_ID_CNV1001, "Num Threads: " + numberOfThread)
    for(i<-iffMetadata.getBody.fields){
      i.initExpression
    }
    println("************************ version time 2017-01-12 17:00 ***************************")
    result
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
    run(jobConfig)
  }

}

trait ParquetConversionOnSparkJob[T<:BaseConversionOnSparkConfig] extends BaseConversionOnSparkJob[T] with Serializable{
  override protected def saveRdd(rdd:RDD[String],targetPath:String): Unit ={
    val fieldDelimiter = this.fieldDelimiter
    val fields: List[IFFField] = iffMetadata.getBody.fields.filter(!_.filter)
    val basePk2Map= (x:String) => {
      val rowData = x.split(fieldDelimiter)
      val array = new ArrayBuffer[String]
      for(v<-rowData){
        array += v
      }
      Row.fromSeq(array)
    }
    val structFields = new util.ArrayList[StructField]()
    for(f <- fields) {
      structFields.add(DataTypes.createStructField(f.name, DataTypes.StringType, true))
    }
    val sqlContext = new SQLContext(sparkContext)
    val structType = DataTypes.createStructType(structFields)
    val rddN = rdd.map(basePk2Map)
    val df = sqlContext.createDataFrame(rddN,structType)
    df.write.mode(SaveMode.Append).parquet(targetPath)
  }

  override protected def saveTargetData():Unit={
    if(iffConversionConfig.autoDeleteTargetDir)cleanDataFile()
    iffConversionConfig.iffFileMode match {
      case FileMode.LOCAL =>
        val fileStrack:mutable.Stack[File] = new mutable.Stack[File]()
        fileStrack.push(new File(getTempDir))
        var index:Int = 0
        var commonMetadataFlag = false
        var metadataFlag = false
        while(!fileStrack.isEmpty){
          val file = fileStrack.pop()
          if(file.isDirectory){
            val fs = file.listFiles()
            for(f<-fs){
              fileStrack.push(f)
            }
          }else if(file.length()>0){
            val srcPath = file.getPath
            if(srcPath.toString.endsWith("_common_metadata")) {
              if(!commonMetadataFlag) {
                val fileName =  "%s/%s".format(iffConversionConfig.datFileOutputPath,"_common_metadata")
                val df = new File(fileName)
                if(!df.exists()) {
                  file.renameTo(df)
                }
                commonMetadataFlag = true
              }
            }else if(srcPath.toString.endsWith("_metadata")) {
              if(!metadataFlag) {
                val fileName =  "%s/%s".format(iffConversionConfig.datFileOutputPath,"_metadata")
                val df = new File(fileName)
                if(!df.exists()) {
                  file.renameTo(df)
                }
                metadataFlag = true
              }
            }else{
              val fileName =  "%s/%s-%05d%s".format(iffConversionConfig.datFileOutputPath,sparkContext.applicationId, index,".gz.parquet")
              file.renameTo(new File(fileName))
              index+=1
            }
          }
        }
      case _ =>
        val fileSystem = FileSystem.get(sparkContext.hadoopConfiguration)
        val sourceFilePath = new Path(getTempDir)
        val fileStatus = fileSystem.getFileStatus(sourceFilePath)
        val fileStatusStrack:mutable.Stack[FileStatus] = new mutable.Stack[FileStatus]()
        fileStatusStrack.push(fileStatus)
        var index:Int = 0;
        var commonMetadataFlag = false
        var metadataFlag = false
        while(!fileStatusStrack.isEmpty){
          val fst = fileStatusStrack.pop()
          if(fst.isDirectory){
            val fileStatusS = fileSystem.listStatus(fst.getPath)
            for(f<-fileStatusS){
              fileStatusStrack.push(f)
            }
          }else if(fst.getLen>0){
            val srcPath = fst.getPath
            if(srcPath.toString.endsWith("_common_metadata")) {
              if(!commonMetadataFlag) {
                val fileName =  "%s/%s".format(iffConversionConfig.datFileOutputPath,"_common_metadata")
                val dstPath = new Path(fileName)
                if(!fileSystem.exists(dstPath)) {
                  DFSUtils.moveFile(fileSystem, srcPath, dstPath)
                }
                commonMetadataFlag = true
              }
            }else if(srcPath.toString.endsWith("_metadata")) {
              if(!metadataFlag) {
                val fileName =  "%s/%s".format(iffConversionConfig.datFileOutputPath,"_metadata")
                val dstPath = new Path(fileName)
                if(!fileSystem.exists(dstPath)) {
                  DFSUtils.moveFile(fileSystem, srcPath, dstPath)
                }
                metadataFlag = true
              }
            }else{
              val fileName =  "%s/%s-%05d%s".format(iffConversionConfig.datFileOutputPath,sparkContext.applicationId, index,".gz.parquet")
              val dstPath = new Path(fileName)
              DFSUtils.moveFile(fileSystem,srcPath, dstPath)
              index+=1
            }
          }
        }
    }

  }

}

