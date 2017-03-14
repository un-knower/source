package com.fsm.model

/**
  * Created by xp on 2017/2/15
  * Modified by xp on 2017/2/22
  */
class Transition(id:String,name:String,state:State) extends Serializable{

  //@BeanProperty
  var transitionId: String = id

  //@BeanProperty
  var transitionName: String = name

  //@BeanProperty
  var toState: State = state

  def this(id:String,name:String)={this(id,name,null)}

}