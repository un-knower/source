package com.context

import com.model.StageInfo
import java.util.{Collections, Comparator, List}

import com.boc.iff.exception.StageInfoErrorException
import org.apache.commons.lang3.StringUtils

import scala.collection.mutable

/**
  * Created by scutlxj on 2017/3/9.
  */
class StageEngine private(){
  private var stageInfos: List[StageInfo] = _
  private var currentProcessIndex: Int = -1
  private var nextIndex: Int = 0

  def this(stageInfos: List[StageInfo]) = {
    this()
    this.stageInfos = stageInfos
    val stageMap: mutable.HashMap[String, StageInfo] = new mutable.HashMap[String, StageInfo]
    for (i <- 0 until stageInfos.size()) {
      val s = stageInfos.get(i)
      if (stageMap.contains(s.stageId)) {
        throw new StageInfoErrorException("StageID [%s] Duplicate Defined")
      }
      stageMap += (s.stageId -> s)
    }
    analysisDepth(stageInfos.get(0), stageMap)
    Collections.sort(this.stageInfos, new Comparator[StageInfo] {
      override def compare(o1: StageInfo, o2: StageInfo): Int = {
        o1.depth.compareTo(o2.depth)
      }
    })
  }

  protected def analysisDepth(startStage: StageInfo, stageMap: mutable.HashMap[String, StageInfo]): Unit = {
    if(StringUtils.isNotEmpty(startStage.nextStageId)){
      val depth = startStage.depth + 1
      if (depth > stageInfos.size()) {
        throw new StageInfoErrorException("This batch job is Dead cycle")
      }
      val stageIds = StringUtils.split(startStage.nextStageId, ",")
      for (s <- stageIds) {
        val st = stageMap(s)
        if (st.depth < depth) {
          st.depth = depth
          analysisDepth(st, stageMap)
        }
      }
    }
  }

  def hasMoreStage():Boolean={
    nextIndex < stageInfos.size()
  }

  def nextStage(): StageInfo = {
    val stage = if (nextIndex >= stageInfos.size()) {
      null
    } else {
      currentProcessIndex += 1
      stageInfos.get(nextIndex)
    }
    nextIndex += 1
    stage
  }

  def currentStage(): StageInfo = {
    if (currentProcessIndex >= stageInfos.size()) {
      null
    } else {
      stageInfos.get(currentProcessIndex)
    }
  }

}
