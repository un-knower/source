package com.context

import com.datahandle._

/**
  * Created by cvinc on 2016/6/8.
  */
object HandleContext {
    implicit object sqlStageHandle extends SqlStageHandle[SqlStageRequest]
    implicit object fileReadStageHandle extends FileReadStageHandle[FileReadStageRequest]
    implicit object fileSaveStageHandle extends FileSaveStageHandle[FileSaveStageRequest]

    def apply[T<:StageRequest](implicit handle: StageHandle[T]) = {
        handle
    }

}
