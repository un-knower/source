package com.context

import com.datahandle._

/**
  * Created by cvinc on 2016/6/8.
  */
object HandleContext {
    implicit object fileReadStageHandle extends FileReadStageHandle[FileReadStageRequest]
    implicit object fileSaveStageHandle extends FileSaveStageHandle[FileSaveStageRequest]
    implicit object aggregateStageHandle extends AggregateStageHandle[AggregateStageRequest]
    implicit object joinStageHandle extends JoinStageHandle[JoinStageRequest]
    implicit object sortStageHandle extends SortStageHandle[SortStageRequest]
    implicit object unionStageHandle extends UnionStageHandle[UnionStageRequest]
    implicit object transformerStageHandle extends TransformerStageHandle[TransformerStageRequest]



    def apply[T<:StageRequest](implicit handle: StageHandle[T]) = {
        handle
    }

}
