package com.fsm.model

import scala.util.control._

/**
  * Created  by xp on 2017/2/15
  * Modified by xp on 2017/2/22
  */
class Process(id:String,name:String) extends Serializable{

  //@BeanProperty
  var processId:String = id

  //@BeanProperty
  var processName:String = name

  //@BeanProperty
  var states:List[State] = Nil

  //@BeanProperty
  var actions:List[Action] = Nil

  //@BeanProperty
  var transitions:List[Transition] = Nil

  def addState(state:State):Boolean = {
    var loop = new Breaks
    var isNew = true
    loop.breakable {
      for(sta<-states){
        if(sta.stateId.equals(state.stateId)){
          isNew = false
          loop.break
        }
      }
    }
    if(isNew)
      states=states:+state
    isNew
  }

  def addAction(action:Action):Boolean = {
    var loop = new Breaks
    var isNew = true
    loop.breakable {
      for(act<-actions){
        if(act.actionId.equals(action.actionId)){
          isNew = false
          loop.break
        }
      }
    }
    if(isNew)
      actions=actions:+action
    isNew
  }

  def addTransition(transition:Transition):Boolean = {
    var loop = new Breaks
    var isNew = true
    loop.breakable {
      for(tran<-transitions){
        if(tran.transitionId.equals(transition.transitionId)){
          isNew = false
          loop.break
        }
      }
    }
    if(isNew)
      transitions=transitions:+transition
    isNew
  }

  def getState(stateId:String):State={
    var loop = new Breaks
    var state:State = null
    loop.breakable {
      for(sta<-states){
        if(sta.stateId.equals(stateId)){
          state = sta
          loop.break
        }
      }
    }
    state
  }

  def getAction(actionId:String):Action={
    var loop = new Breaks
    var action:Action = null
    loop.breakable {
      for(act<-actions){
        if(act.actionId.equals(actionId)){
          action = act
          loop.break
        }
      }
    }
    action
  }
 
  def addTransition(stateId:String,actionId:String,condition:String,toStateId:String):Boolean={
    var isSuccess:Boolean = false
    val state = getState(stateId)
    val action = getAction(actionId)
    val toState = getState(toStateId)
    if(state!=null && action!=null && toState!=null){
      val t = new Transition(""+(transitions.size+1),condition,toState)
      if(action.addTransition(t)){
        if(state.addAction(action)){
          if(addTransition(t))
            isSuccess = true
        }
      }
    }
    isSuccess
  }
 
}