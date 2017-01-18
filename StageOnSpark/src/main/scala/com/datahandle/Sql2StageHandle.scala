package com.datahandle

import com.context.{StageAppContext, StageRequest}

/**
  * Created by cvinc on 2016/6/8.
  */
class Sql2StageHandle[T<:StageRequest] extends StageHandle[T] {
  override def doCommand(stRequest: StageRequest)(implicit appContext:StageAppContext): Unit = {
    println("hello sql2")
  }
}
