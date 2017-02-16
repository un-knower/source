package com.core

import com.context.{StageAppContext, _}
import com.datahandle.StageHandle
import org.apache.commons.lang3.StringUtils

/**
 * Created by cvinc on 2016/6/8.
 */
class AppController {
  def execute(context: StageAppContext) = {
    implicit val stageAppContext = context
    var stageInfo = stageAppContext.fistStage
    do{
      println("**************************handling stage["+stageInfo.stageId+"]**********************************")
      val request: StageRequest = stageInfo.getStageRequest
      val executeHandle = findHandle(request)
      stageAppContext.currentStage = stageInfo
      executeHandle.doCommand(request)
      if(StringUtils.isNotEmpty(stageInfo.nextStageId)){
        stageInfo = stageAppContext.stagesMap.get(stageInfo.nextStageId)
      }else{
        stageInfo = null
      }
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
    }
  }


  private def getHandle[T <: StageRequest]()
                                          (implicit handle: StageHandle[T]): StageHandle[T] = {
    handle
  }

}
