package com.dataframe.part10.IngestionPOC.workflow

/**
  * Created by kalit_000 on 6/8/19.
  */

object ApplicationConfig extends Enumeration {
  type Name = Value
  val
  WORKFLOWNAME,TOEMAIL,FROMEMAIL,BUSDATE,AUDITTABLE,METADATAFILENAME,
  READKAFKA,READJDBC,READSALESFORCE,READS3,READFTP,READAZURE,READGCP,
  SQLETL,
  WRITEKAFKA,WRITES3,WRITEJDBC,WRITESALESFORCE,WRITESFTP,WRITEFTP,WRITEAZURE,WRITEGCP,
  ACESSID,SECRETKEY,SOURCES3PATH,
  ETLSQL,COMPUTESTATS,SQLOPTIONS,
  EXTRACTANDEMAIL,EXTRACTANDEMAILSQL,
  COMPLETEAUDITSQL,COMPLETEAUDIT,
  SOURCEKAFAKATOPIC,SOURCEKAFKABOOTSTRAPSERVER,SOURCEKAFAKASCHEMA,
  TARGETKAFAKATOPIC,TARGETKAFKABOOTSTRAPSERVER,TARGETKAFAKASCHEMA
  = Value

  var param         =  collection.mutable.Map[Name,String]()
  var argValueMap   =  collection.mutable.Map[Name,String]()
  var compNamesMap =  collection.mutable.Map[Name,String]()
}

class IngestionEngineDriverApplication {

}


object IngestionEngineDriver {

  def main(args: Array[String]): Unit = {




  }

}
