package com.fsm.model

import scala.util.control._

/**
  * Created by xp on 2017/2/23
  * Modified by xp on 017/2/23
  */
class FsmInfo(id:String,name:String) extends Serializable{
  //@BeanProperty
  var fsmId:String = id

  //@BeanProperty
  var fsmName:String = name

  //@BeanProperty
  var processes:List[Process] = Nil

  def addProcess(process:Process):Boolean = {
    var loop = new Breaks
    var isNew = true
    loop.breakable {
      for(pro<-processes){
        if(pro.processId.equals(process.processId)){
          isNew = false
          loop.break
        }
      }
    }
    if(isNew)
      processes=processes:+process
    isNew
  }

}