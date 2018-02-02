package com.snowplowanalytics.snowplow.enrich.stream
package good

import org.apache.commons.codec.binary.Base64
import org.specs2.mutable.Specification
import org.json4s.{DefaultFormats, JString}
import org.json4s.jackson.JsonMethods._
import SpecHelpers._

class JsonOutputSpec extends Specification {

  implicit val formats: DefaultFormats.type = DefaultFormats

  "Stream Enrich" should {

    "enrich a valid transaction and produce json with \"event_schema\" field added" in {

      val rawEvent = Base64.decodeBase64(TransactionSpec.raw)

      val enrichedEvent = TestSource.enrichEvents(rawEvent, outputAsJson = true).head
      enrichedEvent.isSuccess must beTrue

      val json = enrichedEvent.toOption.get._1
      val parsedJson = parse(json)
      val eventSchema = parsedJson \ "event_schema"
      eventSchema.isInstanceOf[JString] must beTrue
      eventSchema.extract[String] must_== "iglu:com.snowplowanalytics.snowplow/transaction/jsonschema/1-0-0"
    }
  }
}
