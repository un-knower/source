package com.fsm.model

import scala.util.control._

/**
  * Created by xp on 2017/2/15
  * Modified by xp on 017/2/22
  */
class State(id:String,name:String) extends Serializable{

  //@BeanProperty
  var stateId:String = id

  //@BeanProperty
  var stateName:String = name

  //@BeanProperty
  var actions:List[Action] = Nil

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
  
}