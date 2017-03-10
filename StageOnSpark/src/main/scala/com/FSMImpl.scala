package com.fsm

import scala.collection.mutable._
import com.fsm.model._

/**
  * Created by feisp.p on 2017/2/23
  */
class FSMImpl(fsmInfo:FsmInfo) extends FSMBase with FSMProcessData{
   //构造Process数据
   try{
     buildTreeHashFsm(fsmInfo.processes)
   }catch{
     case t: Throwable =>
       t.printStackTrace()
   }
   override def configProcessData(){}
 }

object FSMImpl extends App{
 /*
      val process1 = new Process("p1","p1")

      addState(process1,new State("A","A"))
      addState(process1,new State("B","B"))
      addState(process1,new State("C","C"))
      addState(process1,new State("D","D"))

      addAction(process1,new Action("a","a"))
      addAction(process1,new Action("b","b"))
      addAction(process1,new Action("c","c"))
      addAction(process1,new Action("d","d"))

      if(addTransition(process1,"A","a","T1","B"))println("add transition ok!")
      if(addTransition(process1,"A","a","T2","A"))println("add transition ok!")
      if(addTransition(process1,"A","b","T3","C"))println("add transition ok!")
      if(addTransition(process1,"A","b","T4","D"))println("add transition ok!")
      if(addTransition(process1,"B","c","T5","C"))println("add transition ok!")
      if(addTransition(process1,"B","c","T6","B"))println("add transition ok!")
      if(addTransition(process1,"B","d","T7","A"))println("add transition ok!")
      if(addTransition(process1,"B","d","T8","D"))println("add transition ok!")

      FsmInfo fsmInfo = new FsmInfo("1","fsminfo1")
      fsmInfo.addProcess(process1)

      FSMImpl fsm = new FSMImpl(fsmInfo)
      println("(p1,A,a,T1)'s next state is: "+fsm.getNextState("p1","A","a","T1"))
*/

}


