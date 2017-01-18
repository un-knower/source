package com.datahandle

import com.context.{SqlRequest2, SqlRequest1, StageRequest}

/**
  * Created by cvinc on 2016/6/8.
  */
object HandleContext {
    implicit object sql1 extends Sql1StageHandle[SqlRequest1]
    implicit object sql2 extends Sql2StageHandle[SqlRequest2]

    def apply[T<:StageRequest](implicit handle: StageHandle[T]) = {
        handle
    }

}
