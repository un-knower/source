package com.datahandle

import com.context._
import com.core.AppController


/**
  * Created by cvinc on 2016/6/8.
  */
object  MainTest {
    def main(args: Array[String]) {
       val stageRequest=new FileSaveStageRequest
        val executeHandle = findHandle(stageRequest)
        implicit val context1 = new StageAppContext(null,null)
        executeHandle.doCommand(stageRequest)
    }

    def findHandle(request: StageRequest) = {
        import HandleContext._
        request match {
            case request: SqlStageRequest => getHandle[SqlStageRequest]()
            case request: FileSaveStageRequest => getHandle[FileSaveStageRequest]()
        }
    }

    private def getHandle[T <: StageRequest]()
                                            (implicit handle: StageHandle[T]): StageHandle[T] = {
        handle
    }
}
