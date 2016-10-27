package com.boc.iff

import com.boc.iff.IFFConversion._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

/**
  * Created by cvinc on 2016/6/26.
  */
object DFSUtils {

  private val logger = new ECCLogger()

  object FileMode extends Enumeration {
    type ValueType = Value
    val LOCAL = Value("local")
    val DFS = Value("dfs")
  }

  def moveFile(srcPath: Path, dstPath: Path)(implicit configuration: Configuration): Unit = {
    val fileSystem = FileSystem.get(configuration)
    var renameCompleted = false
    var tryTimes = 0
    while (!renameCompleted && tryTimes < 5) {
      logger.info(MESSAGE_ID_CNV1001, "Move [%s] To [%s]...".format(srcPath.toString, dstPath.toString))
      try {
        renameCompleted = fileSystem.rename(srcPath, dstPath)
      } catch {
        case e: Exception =>
          logger.warn(MESSAGE_ID_CNV1001, e.getMessage)
          logger.info(MESSAGE_ID_CNV1001, "Retry...")
          Thread.sleep(1000)
          tryTimes += 1
      }
    }
  }


  def deleteDir(path: String)(implicit configuration: Configuration): Unit = {
    val fileSystem = FileSystem.get(configuration)
    val fsPath = new Path(path)
    println("*********************delete dir:"+path)
    if(fileSystem.exists(fsPath)){
      if(fileSystem.delete(fsPath, true)) {
        println("*********************delete dir:"+path+"success********************")
      }
    }
  }

  def checkDir(path: String)(implicit configuration: Configuration): Boolean = {
    var fieExist = false
    val fileSystem = FileSystem.get(configuration)
    val fsPath = new Path(path)
    if(fileSystem.exists(fsPath)){
      fieExist = true
    }
    fieExist
  }

  def createDir(path: String)(implicit configuration: Configuration): Unit = {
    if(!checkDir(path))FileSystem.get(configuration).mkdirs(new Path(path))
  }

}
