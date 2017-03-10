package com.datahandle.load

import java.util

import com.boc.iff.model.IFFField
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{DataTypes, StructField}

/**
  * Created by scutlxj on 2017/2/9.
  */
class ParquetFileLoader extends FileLoader{

  def loadFile(): DataFrame = {
    val df = this.sqlContext.read.parquet(fileInfo.dataPath)
    val cols = df.columns
    val structFields = new util.ArrayList[StructField]()
    for(name <- cols) {
      structFields.add(DataTypes.createStructField(name.toUpperCase, DataTypes.StringType, true))
    }
    val structType = DataTypes.createStructType(structFields)
    this.sqlContext.createDataFrame(df.rdd,structType)
  }

}
