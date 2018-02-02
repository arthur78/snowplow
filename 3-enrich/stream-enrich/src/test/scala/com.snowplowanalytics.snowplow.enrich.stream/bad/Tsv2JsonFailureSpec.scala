package com.snowplowanalytics.snowplow.enrich.stream
package bad

import org.apache.commons.codec.binary.Base64
import org.specs2.mutable.Specification
import org.json4s.DefaultFormats
import good.TransactionSpec
import org.specs2.mock.Mockito
import SpecHelpers._


class Tsv2JsonFailureSpec extends Specification with Mockito {

  implicit val formats: DefaultFormats.type = DefaultFormats

  "Stream Enrich" should {

    "should return a failure event in case of Tsv2Json error" in {
      val s = spy(TestSource)
      org.mockito.Mockito.doReturn(Left(List("The error message."))).when(s)._tsv2json(anyString)
      val rawEvent = Base64.decodeBase64(TransactionSpec.raw)
      val outputEvent = s.enrichEvents(rawEvent, outputAsJson = true).head
      outputEvent.isFailure must beTrue
    }
  }
}
