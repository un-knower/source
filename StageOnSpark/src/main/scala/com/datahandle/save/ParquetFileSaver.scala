package com.datahandle.save

import org.apache.spark.sql.{DataFrame, Row}

/**
  * Created by scutlxj on 2017/2/10.
  */
class ParquetFileSaver extends FileSaver{
  override protected def saveDataFrame(path:String,df: DataFrame): Unit = {
    df.write.parquet(path)
  }

}
