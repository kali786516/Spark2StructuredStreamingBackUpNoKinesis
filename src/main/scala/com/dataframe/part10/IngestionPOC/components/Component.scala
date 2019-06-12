package com.dataframe.part10.IngestionPOC.components

/**
  * Created by kalit_000 on 6/11/19.
  */

import com.dataframe.part10.IngestionPOC.workflow.ApplicationConfig

abstract class Component {

  def init(enum:ApplicationConfig.type,metaDataTableName:String)

}
