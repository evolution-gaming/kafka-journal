package com.evolution.kafka.journal

import org.scalacheck.Gen
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks
import play.api.libs.json.*

import java.nio.charset.StandardCharsets.UTF_8
import scala.util.Try

class HeadActionSpec extends AnyFunSuite with Matchers with ScalaCheckPropertyChecks {
  import HeadActionSpec.*

  implicit override val generatorDrivenConfig: PropertyCheckConfiguration =
    PropertyCheckConfiguration(minSuccessful = 2000)

  /**
   * The full decoder, [[ActionHeader.formatOptActionHeader]]: `None` on failure.
   */
  private def full(json: String): Option[Option[HeadAction]] = {
    Try {
      ActionHeader
        .formatOptActionHeader
        .reads(Json.parse(json))
        .asOpt
        .map { _.map { HeadAction(_) } }
    }.toOption.flatten
  }

  private def fast(json: String): Option[Option[HeadAction]] = HeadAction.fromJson(json.getBytes(UTF_8))

  private def resource(name: String): String = {
    val stream = getClass.getResourceAsStream(name)
    try new String(stream.readAllBytes(), UTF_8)
    finally stream.close()
  }

  private val examples = for {
    action <- List("Append", "Delete", "Purge", "Mark")
    origin <- List("None", "origin")
    suffix <- action match {
      case "Append" =>
        for {
          payloadType <- List("Binary", "Json")
          metadata <- List("legacy", "metadata", "none")
        } yield s"-$payloadType-$metadata"
      case _ => List("")
    }
  } yield s"$action-$origin$suffix.json"

  examples.foreach { name =>
    test(s"decodes $name same as the full decoder") {
      val json = resource(name)
      val expected = full(json)
      expected should not be empty
      fast(json) shouldEqual expected
    }
  }

  test("decodes the fields used by the head cache") {
    fast("""{"append":{"range":{"from":3,"to":7},"payloadType":"json"}}""") shouldEqual
      Some(Some(HeadAction.Append(SeqRange.unsafe(3, 7))))
    fast("""{"delete":{"to":5}}""") shouldEqual Some(Some(HeadAction.Delete(SeqNr.unsafe(5).toDeleteTo)))
    fast("""{"purge":{}}""") shouldEqual Some(Some(HeadAction.Purge))
    fast("""{"mark":{"id":"a"}}""") shouldEqual Some(Some(HeadAction.Mark))
    fast("""{}""") shouldEqual Some(None)
  }

  test("skips metadata, also when the full decoder cannot parse it") {
    val json =
      s"""{"append":{"range":{"from":1,"to":2},"payloadType":"json","metadata":{"data":{"amount":1${ "0" * 400 }}}}}"""
    full(json) shouldEqual None
    fast(json) shouldEqual Some(Some(HeadAction.Append(SeqRange.unsafe(1, 2))))
  }

  // For each of these the fast path must either give the result of the full decoder,
  // or return `None`, so that the caller uses the full decoder.
  private val edgeCases = List(
    """{"append":{"range":{"from":1,"to":5},"payloadType":"json","origin":null,"version":"1"}}""",
    """{"append":{"payloadType":"binary","range":{"to":5,"from":1},"extra":[1,{"a":2}]}}""",
    """{"append":{"range":{"from":5,"to":1},"payloadType":"json"}}""",
    """{"append":{"range":{"from":0,"to":1},"payloadType":"json"}}""",
    """{"append":{"range":{"from":1.0,"to":5},"payloadType":"json"}}""",
    """{"append":{"range":{"from":1},"payloadType":"json"}}""",
    """{"append":{"range":{"from":1,"to":5}}}""",
    """{"append":{"range":{"from":1,"to":5},"payloadType":"text"}}""",
    """{"append":{"range":{"from":1,"to":5},"payloadType":null}}""",
    """{"append":{"range":{"from":1,"to":5},"payloadType":"json","origin":1}}""",
    """{"append":{"range":{"from":1,"to":5},"payloadType":"json","metadata":{"data":null}}}""",
    """{"append":{"range":{"from":1,"to":5},"payloadType":"json","metadata":1}}""",
    """{"append":{"range":{"from":1,"to":5},"range":{"from":1,"to":6},"payloadType":"json"}}""",
    """{"append":1}""",
    """{"append":{"range":{"from":1,"to":5},"payloadType":"json"},"mark":{"id":"a"}}""",
    """{"mark":{"id":"a"},"append":{"range":{"from":1,"to":5},"payloadType":"json"}}""",
    """{"append":{"range":{"from":1,"to":5},"payloadType":"json"},"unknown":1}""",
    """{"unknown":{}}""",
    """{"delete":{"to":0}}""",
    """{"delete":{}}""",
    """{"delete":{"to":3,"version":2}}""",
    """{"mark":{}}""",
    """{"mark":{"id":1}}""",
    """{"mark":{"id":null}}""",
    """{"purge":{"origin":"o","version":null}}""",
    """{"purge":{"origin":[]}}""",
    """{"purge":[]}""",
    """null""",
    """[]""",
    """{"purge":{}} trailing""",
    """{"purge":{}""",
  )

  edgeCases.foreach { json =>
    test(s"agrees with the full decoder or falls back: $json") {
      fast(json).foreach { result =>
        full(json) shouldEqual Some(result)
      }
    }
  }

  test("decodes every header written by the journal, without the fallback") {
    forAll(genActionHeader) { header =>
      val json = Json.stringify(Json.toJson(header))
      fast(json) shouldEqual Some(Some(HeadAction(header)))
    }
  }

  test("agrees with the full decoder or falls back, for any JSON made of header keys") {
    forAll(genHeaderLikeJson, minSuccessful(20000)) { json =>
      val json1 = Json.stringify(json)
      fast(json1).foreach { result =>
        full(json1) shouldEqual Some(result)
      }
    }
  }
}

object HeadActionSpec {

  // the edge values of a SeqNr, and random ones
  private val genSeqNr: Gen[Long] = Gen.oneOf(Gen.const(1L), Gen.const(Long.MaxValue), Gen.chooseNum(1L, Long.MaxValue))

  private val genSeqRange: Gen[SeqRange] = for {
    a <- genSeqNr
    b <- genSeqNr
  } yield SeqRange(SeqNr.unsafe(a min b), SeqNr.unsafe(a max b))

  // any text, including quotes, escapes and non-ASCII characters
  private val genString: Gen[String] = Gen.oneOf(Gen.alphaNumStr, Gen.asciiStr, Gen.listOf(Gen.oneOf(Gen.alphaChar, Gen.oneOf('"', '\\', '\n', 'é', '中', '\u0001'))).map(_.mkString))

  private val genOrigin: Gen[Option[Origin]] = Gen.option(genString.map(Origin(_)))

  private val genVersion: Gen[Option[Version]] = Gen.option(genString.map(Version(_)))

  private def genJsValue(depth: Int): Gen[JsValue] = {
    val leaf: Gen[JsValue] = Gen.oneOf(
      Gen.const(JsNull),
      Gen.oneOf(JsTrue, JsFalse),
      Gen.chooseNum(Long.MinValue, Long.MaxValue).map(a => JsNumber(BigDecimal(a))),
      Gen.chooseNum(-1e9, 1e9).map(a => JsNumber(BigDecimal(a))),
      genString.map(JsString(_)),
    )
    if (depth <= 0) leaf
    else {
      Gen.frequency(
        3 -> leaf,
        1 -> Gen.listOfN(3, genJsValue(depth - 1)).map(JsArray(_)),
        1 -> Gen.listOfN(3, Gen.zip(genString, genJsValue(depth - 1))).map(JsObject(_)),
      )
    }
  }

  private val genHeaderMetadata: Gen[HeaderMetadata] = Gen.oneOf(
    Gen.const(HeaderMetadata.empty),
    genJsValue(3).map(a => HeaderMetadata(Some(a))),
  )

  val genActionHeader: Gen[ActionHeader] = Gen.oneOf(
    for {
      range <- genSeqRange
      origin <- genOrigin
      version <- genVersion
      payloadType <- Gen.oneOf(PayloadType.Binary, PayloadType.Json)
      metadata <- genHeaderMetadata
    } yield ActionHeader.Append(range, origin, version, payloadType, metadata),
    for {
      id <- genString
      origin <- genOrigin
      version <- genVersion
    } yield ActionHeader.Mark(id, origin, version),
    for {
      to <- genSeqNr
      origin <- genOrigin
      version <- genVersion
    } yield ActionHeader.Delete(SeqNr.unsafe(to).toDeleteTo, origin, version),
    for {
      origin <- genOrigin
      version <- genVersion
    } yield ActionHeader.Purge(origin, version),
  )

  private val keys = List(
    "append", "mark", "delete", "purge", "range", "from", "to", "id", "payloadType", "origin", "version",
    "metadata", "data", "other",
  )

  // values that are valid somewhere in a header, and values that are not
  private val genHeaderValue: Gen[JsValue] = Gen.oneOf(
    Gen.chooseNum(-1L, 5L).map(a => JsNumber(BigDecimal(a))),
    Gen.const(JsNumber(BigDecimal("1.5"))),
    Gen.oneOf("json", "binary", "text", "id").map(JsString(_)),
    Gen.const(JsNull),
    Gen.const(JsTrue),
    Gen.const(JsArray()),
  )

  private def genHeaderLike(depth: Int): Gen[JsValue] = {
    val obj = Gen
      .choose(0, 4)
      .flatMap { n => Gen.listOfN(n, Gen.zip(Gen.oneOf(keys), if (depth <= 0) genHeaderValue else genHeaderLike(depth - 1))) }
      .map(JsObject(_))
    if (depth <= 0) genHeaderValue else Gen.frequency(1 -> genHeaderValue, 4 -> obj)
  }

  val genHeaderLikeJson: Gen[JsValue] = Gen.frequency(
    // mostly a valid header with one change, then anything made of the header keys
    3 -> Gen.zip(genActionHeader, Gen.oneOf(keys), Gen.frequency(3 -> genHeaderValue, 1 -> genHeaderLike(1))).map {
      case (header, key, value) =>
        Json.toJson(header) match {
          case JsObject(fields) =>
            val (action, body) = fields.head
            body match {
              case body: JsObject => JsObject(Seq(action -> (body + (key -> value))))
              case body => JsObject(Seq(action -> body))
            }
          case other => other
        }
    },
    2 -> genHeaderLike(3),
  )
}
