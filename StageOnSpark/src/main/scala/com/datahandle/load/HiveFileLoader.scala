package com.datahandle.load

import org.apache.spark.sql.DataFrame

/**
  * Created by scutlxj on 2017/2/9.
  */
class HiveFileLoader extends FileLoader{

  def loadFile(): DataFrame = {
    println("***********************load hive file *****************************")
    val targetRdd = this.sparkContext.textFile(this.fileInfo.dataPath)
    println("***********************file"+targetRdd.count()+" *****************************")
    changeRddToDataFrame(targetRdd)
  }

}
