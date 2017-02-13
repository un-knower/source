package com.datahandle

import com.context.{StageAppContext, StageRequest}

/**
  * Created by cvinc on 2016/6/8.
  */
class SqlStageHandle[T<:StageRequest] extends StageHandle[T] {
  override def doCommand(stRequest: StageRequest)(implicit appContext:StageAppContext): Unit = {
    println("hello sql1")
  }
}



