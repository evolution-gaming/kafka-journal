package com.evolution.kafka.journal

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import play.api.libs.json.Json

import java.nio.charset.StandardCharsets
import scala.io.{Codec, Source}
import scala.util.{Success, Try, Using}

/**
 * [[ActionHeaderJson]] must decode the JSON written by the journal, and, whenever it decodes
 * anything else, give the same result as [[ActionHeader.formatOptActionHeader]].
 */
class ActionHeaderJsonDecodeSpec extends AnyFunSuite with Matchers {
  import ActionHeaderJsonDecodeSpec.*

  private val fixtures = for {
    origin <- Vector("origin", "None")
    name <- Vector("Binary", "Json").flatMap { payloadType =>
      Vector("metadata", "none", "legacy").map { metadata => s"Append-$origin-$payloadType-$metadata" }
    } ++ Vector(s"Delete-$origin", s"Purge-$origin", s"Mark-$origin")
  } yield name

  fixtures.foreach { name =>
    test(s"decode $name.json the same as play-json") {
      val json = resource(s"$name.json")
      val expected = viaPlayJson(json)
      expected.toOption.flatten should not be empty
      decode(json) shouldEqual expected
    }
  }

  // JSON decoded without falling back
  List(
    """{}""",
    """{"unknown":{"a":[1,2,{"b":null}]}}""",
    """{"unknown":1,"purge":{}}""",
    """{"purge":{"unknown":[1,"2"]}}""",
    """{"purge":{"origin":"oä\"\\","version":"1"}}""",
    """{"append":{"range":{"from":1,"to":2},"payloadType":"binary"}}""",
    """{"append":{"payloadType":"json","range":{"to":9223372036854775807,"from":1}}}""",
    """{"append":{"range":{"from":1,"to":2},"payloadType":"json","metadata":{}}}""",
    """{"append":{"range":{"from":1,"to":2},"payloadType":"json","metadata":{"data":null}}}""",
    """{"append":{"range":{"from":1,"to":2},"payloadType":"json","metadata":{"data":{"k":[1.5,true,"v"]}}}}""",
    """{"append":{"range":{"from":1,"to":2},"payloadType":"json","metadata":{"data":"x","unknown":1}}}""",
    """{"delete":{"to":3,"origin":"o"}}""",
    """{"mark":{"id":"id","version":"1"}}""",
    """ { "mark" : { "id" : "id" } } """,
  ).foreach { json =>
    test(s"decode the same as play-json: $json") {
      val expected = viaPlayJson(json)
      expected shouldBe a[Success[?]]
      decode(json) shouldEqual expected
    }
  }

  // JSON left to play-json: either valid, but with semantics the decoder does not replicate, or invalid
  List(
    """[]""",
    """1""",
    """null""",
    """{"purge":{},"mark":{"id":"id"}}""",
    """{"purge":{},"purge":{"origin":"o"}}""",
    """{"purge":1}""",
    """{"purge":null}""",
    """{"purge":{"origin":"a","origin":"b"}}""",
    """{"purge":{"origin":1}}""",
    """{"purge":{"origin":null,"version":null}}""",
    """{"append":{"range":{"from":1.0,"to":2},"payloadType":"json"}}""",
    """{"append":{"range":{"from":1,"to":2},"payloadType":"text"}}""",
    """{"append":{"range":{"from":1,"to":2},"payloadType":"json","metadata":1}}""",
    """{"append":{"range":{"from":1,"to":2},"payloadType":"json","metadata":null}}""",
    """{"append":{"range":{"from":1},"payloadType":"json"}}""",
    """{"append":{"range":{"from":0,"to":2},"payloadType":"json"}}""",
    """{"append":{"payloadType":"json"}}""",
    """{"delete":{"to":0}}""",
    """{"delete":{}}""",
    """{"mark":{}}""",
    """{"purge":{}} trailing""",
    """{"purge":{}""",
  ).foreach { json =>
    test(s"leave to play-json: $json") {
      decode(json).isFailure shouldEqual true
      // whatever play-json does with it is the result, the decoder must not claim it
      noException should be thrownBy viaPlayJson(json)
    }
  }
}

object ActionHeaderJsonDecodeSpec {

  def bytesOf(json: String): Array[Byte] = json.getBytes(StandardCharsets.UTF_8)

  def decode(json: String): Try[Option[ActionHeader]] = Try { ActionHeaderJson.decode(bytesOf(json)) }

  def viaPlayJson(json: String): Try[Option[ActionHeader]] = Try {
    ActionHeader.formatOptActionHeader.reads(Json.parse(json)).get
  }

  def resource(name: String): String = {
    Using.resource(Source.fromResource(s"com/evolution/kafka/journal/$name")(Codec.UTF8)) { _.mkString }
  }
}
