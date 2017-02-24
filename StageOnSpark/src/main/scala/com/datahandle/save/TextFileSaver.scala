package com.datahandle.save
import org.apache.spark.sql.{DataFrame, Row}

import scala.collection.mutable.ArrayBuffer

/**
  * Created by scutlxj on 2017/2/10.
  */
class TextFileSaver extends FileSaver{
  override protected def saveDataFrame(path:String,df: DataFrame): Unit = {
    val targetSeparator = fileInfo.targetSeparator
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
    val rdd = df.rdd.map(rowToString)
    /*if(rdd.getNumPartitions>repartitionNumber){
      rdd.repartition(repartitionNumber)
    }*/
    rdd.saveAsTextFile(path)
  }
}
