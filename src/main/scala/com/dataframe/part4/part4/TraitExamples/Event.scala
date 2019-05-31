package com.dataframe.part4.part4.TraitExamples

/**
  * Created by kalit_000 on 5/24/19.
  */
case class Event (id:Int,location:String,dayOfWeek:String,
                  sessionTimeInSeconds:Integer,source:String)

//val e1 = Event(1,"US","Sun",10,"Twitter")

/* thin interface */
trait EventsInterface {
  def get(eventID:Int): Option[Event]

  def all:List[Event]
}

class Events(val events: List[Event]) extends EventsInterface {
  override def get(eventID:Int) = events.find(x => x.id == eventID)

  override def all: List[Event] = events
}

/*
val events = new Events(List(e1))
events.get(1)
events.all
*/
