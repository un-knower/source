package com.core

import com.boc.iff.exception.BaseException
import com.context._
import com.datahandle.StageHandle

/**
 * Created by cvinc on 2016/6/8.
 */
class AppController {
  def execute(context: StageAppContext) = {
    implicit val stageAppContext = context
    val logBuilder = stageAppContext.constructLogBuilder()
    logBuilder.setLogThreadID(Thread.currentThread().getId.toString)
    while(context.stageEngine.hasMoreStage()){
      val stageInfo = context.stageEngine.nextStage()
      logBuilder.info("handling Stage["+stageInfo.stageId+"]")
      val request: StageRequest = stageInfo.getStageRequest
      val executeHandle = findHandle(request)
      try {
        executeHandle.doCommand(request)
      }catch {
        case e:BaseException => throw e
        case t:Throwable =>
          logBuilder.error("Error case when handling Stage["+stageInfo.stageId+"]")
          throw t
      }
    }
  }

  def findHandle(request: StageRequest) = {
    import HandleContext._
    request match {
      case request: FileReadStageRequest => getHandle[FileReadStageRequest]()
      case request: FileSaveStageRequest => getHandle[FileSaveStageRequest]()
      case request: AggregateStageRequest => getHandle[AggregateStageRequest]()
      case request: JoinStageRequest => getHandle[JoinStageRequest]()
      case request: SortStageRequest => getHandle[SortStageRequest]()
      case request: UnionStageRequest => getHandle[UnionStageRequest]()
      case request: TransformerStageRequest => getHandle[TransformerStageRequest]()
    }
  }


  private def getHandle[T <: StageRequest]()
                                          (implicit handle: StageHandle[T]): StageHandle[T] = {
    handle
  }

}
