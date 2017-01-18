package com.core

import com.context._
import com.datahandle.StageHandle

/**
 * Created by cvinc on 2016/6/8.
 */
class AppController {
  def execute(context: StageAppContext) = {
    implicit val context1 = context
    val request: StageRequest = new SqlRequest1();
    val executeHandle = findHandle(request)
    executeHandle.doCommand(request)
  }

  def findHandle(request: StageRequest) = {
    import com.datahandle.HandleContext._
    request match {
      case request: SqlRequest2 => getHandle[SqlRequest2]()
      case request: SqlRequest1 => getHandle[SqlRequest1]()
    }
  }


  private def getHandle[T <: StageRequest]()
                                          (implicit handle: StageHandle[T]): StageHandle[T] = {
    handle
  }

}
