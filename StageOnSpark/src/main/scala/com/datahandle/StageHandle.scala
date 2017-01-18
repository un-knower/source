package com.datahandle

import com.context.{StageAppContext, StageRequest}

/**
  * Created by cvinc on 2016/6/8.
  */
trait StageHandle[T<:StageRequest] {
    def doCommand(stRequest:StageRequest)(implicit  context:StageAppContext)
}
