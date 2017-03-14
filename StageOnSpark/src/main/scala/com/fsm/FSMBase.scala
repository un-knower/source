package com.fsm

import scala.collection.mutable._
import com.fsm.model._

/**
  * Created  by xp on 2017/2/22
  * Modified by xp on 2017/2/23
  */
abstract class FSMBase {

  protected var treeHashFsm = Map[String,Map[String,List[Transition]]]]()

  //构建Tree+Hash结构状态机
  def buildTreeHashFsm(processList : List[Process]){
    for(pro<-processList){
      var pmap=Map[String,Map[String,List[Transition]]]()
      treeHashFsm ++= Map(pro.processName -> pmap)
   
      for(sta<-pro.states){
        var smap=Map[String,List[Transition]]()
        pmap ++= Map(sta.stateId -> smap)

        for(action<-sta.actions){
          var transitions = List[Transition]()
       
          for(transition<-action.transitions){
            transitions=transitions:+transition
          }
          smap ++= Map(action.actionId -> transitions)
        }
      }
    }
  }

  //获取下一个状态
  def getNextState(processName:String,stateId:String,actionId:String,transName:String):String = {
    var toState:String = null
 
    try{
      val transitions:List[Transition] = ((treeHashFsm.get(processName)).get(stateId)).get(actionId).get
      
      for(trans<-transitions){
        if(trans.transitionName.equals(transName)){
          toState=trans.toState.stateName
        }
      }
    }catch{
      case t: Throwable =>
        t.printStackTrace()
    }
    return toState
  }
}

trait FSMProcessData{

  //增加状态，成功返回true，失败返回false
  def addState(process:Process,state:State){
    process.addState(state)
  }

  //增加动作，成功返回true，失败返回false
  def addAction(process:Process,action:Action){
    process.addAction(action)
  }

  //增加迁移，根据状态、动作和条件及下一状态构建迁移，成功返回true，失败返回false
  def addTransition(process:Process,stateId:String,actionId:String,condition:String,toStateId:String):Boolean={
    process.addTransition(stateId,actionId,condition,toStateId)
  }

  //配置Process数据，由具体实现类实现
  def configProcessData

}

