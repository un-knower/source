package com.datahandle.load

import org.apache.spark.sql.DataFrame

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
