package com.datahandle.load

import com.boc.iff.model.IFFField
import org.apache.spark.sql.DataFrame

/**
  * Created by scutlxj on 2017/2/9.
  */
class ParquetFileLoader extends FileLoader{

  def loadFile(): DataFrame = {
    this.sqlContext.read.parquet(fileInfo.dataPath)
  }

}
