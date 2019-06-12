package com.dataframe.part10.IngestionPOC.components

/**
  * Created by kalit_000 on 6/11/19.
  */

import com.dataframe.part10.IngestionPOC.components
import com.dataframe.part10.IngestionPOC.workflow.ApplicationConfig

class ReadS3 extends OutputComponent {

  println("I am here at ReadS3 Class")

  override def init(enum: ApplicationConfig.type, metaDataTableName: String): Unit = {

  }

}
