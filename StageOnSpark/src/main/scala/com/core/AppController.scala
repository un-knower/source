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
    var timeStamp:Long = 0
    while(context.stageEngine.hasMoreStage()){
      val stageInfo = context.stageEngine.nextStage()
      logBuilder.info("submit Stage["+stageInfo.stageId+"]")
      val request: StageRequest = stageInfo.getStageRequest
      val executeHandle = findHandle(request)
      try {
        timeStamp = System.currentTimeMillis()
        executeHandle.doCommand(request)
       // logBuilder.info("Stage[%s] finished in %s ms".format(stageInfo.stageId,System.currentTimeMillis-timeStamp))
      }catch {
        case e:BaseException => throw e
        case t:Throwable =>
          //logBuilder.error("Error case when handling Stage["+stageInfo.stageId+"]")
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
      case request: DeduplicateStageRequest => getHandle[DeduplicateStageRequest]()
      case request: LookupStageRequest => getHandle[LookupStageRequest]()
      case request: MergeStageRequest => getHandle[MergeStageRequest]()
    }
  }


  private def getHandle[T <: StageRequest]()
                                          (implicit handle: StageHandle[T]): StageHandle[T] = {
    handle
  }

}
