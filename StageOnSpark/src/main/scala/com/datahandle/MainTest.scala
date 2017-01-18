package com.datahandle

import com.context.{SqlRequest1, StageAppContext}
import com.core.AppController

/**
  * Created by cvinc on 2016/6/8.
  */
object  MainTest {
    def main(args: Array[String]) {
        val stageRequest=new SqlRequest1
        val stageAppContext= new StageAppContext()
        stageAppContext.request=stageRequest
        val controller = new AppController()
        controller.execute(stageAppContext);
    }
}
