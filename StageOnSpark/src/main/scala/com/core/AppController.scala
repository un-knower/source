package com.core

import com.boc.iff.exception.BaseException
import com.context.{StageAppContext, _}
import com.datahandle.StageHandle
import org.apache.commons.lang3.StringUtils

/**
 * Created by cvinc on 2016/6/8.
 */
class AppController {
  def execute(context: StageAppContext) = {
    implicit val stageAppContext = context
    val logBuilder = stageAppContext.constructLogBuilder()
    logBuilder.setLogThreadID(Thread.currentThread().getId.toString)
    var stageInfo = stageAppContext.fistStage
    logBuilder.info("first stage of the job [%s]".format(stageInfo.stageId))
    var finishStageNumber:Int = 0
    val totalStageNumber:Int = stageAppContext.stagesMap.size()
    do{
      logBuilder.info("handling Stage["+stageInfo.stageId+"],Stage[Total:"+totalStageNumber+",Finish:"+finishStageNumber+"]")
      val request: StageRequest = stageInfo.getStageRequest
      val executeHandle = findHandle(request)
      stageAppContext.currentStage = stageInfo
      try {
        executeHandle.doCommand(request)
      }catch {
        case e:BaseException => throw e
        case t:Throwable =>
          logBuilder.error("Error case when handling Stage["+stageInfo.stageId+"]")
          throw t
      }
      if(StringUtils.isNotEmpty(stageInfo.nextStageId)){
        stageInfo = stageAppContext.stagesMap.get(stageInfo.nextStageId)
      }else{
        stageInfo = null
      }
      finishStageNumber+=1
    }while(stageInfo!=null)
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
