package com.fsm.model

import scala.util.control._

/**
  * Created by xp on 2017/2/15
  * Modified by xp on 017/2/22
  */
class Action(id:String,name:String) extends Serializable{

  //@BeanProperty
  var actionId:String = id

  //@BeanProperty
  var actionName:String = name

  //@BeanProperty
  var transitions:List[Transition] = Nil

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

}