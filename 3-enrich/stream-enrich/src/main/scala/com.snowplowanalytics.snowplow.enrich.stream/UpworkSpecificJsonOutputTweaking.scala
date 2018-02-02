package com.snowplowanalytics.snowplow.enrich.stream

import org.json4s.DefaultFormats
import org.json4s.JsonAST.{JObject, JString, JValue}

/**
  * Upwork domain-specific tweaking of the snowplow enriched event output in JSON.
  */
object UpworkSpecificJsonOutputTweaking {

  implicit val formats: DefaultFormats.type = DefaultFormats
  case class Event(event_vendor: String, event_name: String, event_format: String, event_version: String)

  /**
    * Add "event_schema" field into the resulting JSON.
    *
    * @param json Snowplow enriched event as a JSON object
    * @return The given JSON object with added "event_schema" field.
    */
  def addEventSchema(json: JValue): JValue = {
    val e = json.extract[Event]
    val eventSchema = s"iglu:${e.event_vendor}/${e.event_name}/${e.event_format}/${e.event_version}"
    json merge JObject("event_schema" -> JString(eventSchema))
  }
}
