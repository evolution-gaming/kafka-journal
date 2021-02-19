package com.evolutiongaming.kafka.journal

import cats.syntax.all._
import com.evolutiongaming.kafka.journal.ExpireAfter.implicits._
import com.evolutiongaming.kafka.journal.util.PlayJsonHelper._
import org.scalatest.matchers.should.Matchers
import play.api.libs.json.{JsResult, Json}

import scala.concurrent.duration._
import org.scalatest.funsuite.AnyFunSuite

class RecordMetadataTest extends AnyFunSuite with Matchers {

  for {
    (metadata, json) <- List(
      (
        RecordMetadata.empty,
        Json.obj(
          ("header", Json.obj()),
          ("payload", Json.obj()))),
      (
        RecordMetadata(
          HeaderMetadata(Json.obj(("key0", "value0")).some),
          PayloadMetadata(1.day.toExpireAfter.some, Json.obj(("key1", "value1")).some)),
        Json.obj(
          ("header", Json.obj(
            ("data", Json.obj(("key0", "value0"))))),
          ("payload", Json.obj(
            ("expireAfter", "1 day"),
            ("data", Json.obj(("key1", "value1"))))),
          ("data", Json.obj(("key0", "value0"))))),
      (
        RecordMetadata(
          HeaderMetadata(Json.obj(("key0", "value0")).some),
          PayloadMetadata(none, Json.obj(("key1", "value1")).some)),
        Json.obj(
          ("header", Json.obj(
            ("data", Json.obj(("key0", "value0"))))),
          ("payload", Json.obj(
            ("data", Json.obj(("key1", "value1"))))),
          ("data", Json.obj(("key0", "value0"))))),
      (
        RecordMetadata(
          HeaderMetadata.empty,
          PayloadMetadata(1.day.toExpireAfter.some, Json.obj(("key1", "value1")).some)),
        Json.obj(
          ("header", Json.obj()),
          ("payload", Json.obj(
            ("expireAfter", "1 day"),
            ("data", Json.obj(("key1", "value1"))))))),
      (
        RecordMetadata(
          HeaderMetadata(Json.obj(("key0", "value0")).some),
          PayloadMetadata.empty),
        Json.obj(
          ("header", Json.obj(
            ("data", Json.obj(("key0", "value0"))))),
          ("payload", Json.obj()),
          ("data", Json.obj(("key0", "value0"))))))
  } {
    test(s"formatRecordMetadata reads & writes $json") {
      Json.toJson(metadata) shouldEqual json
      json.validate[RecordMetadata] shouldEqual metadata.pure[JsResult]
    }
  }

  for {
    (metadata, json) <- List(
      (
        RecordMetadata.empty,
        Json.obj()),
      (
        RecordMetadata(
          HeaderMetadata(Json.obj(("key0", "value0")).some),
          PayloadMetadata.empty),
        Json.obj(
          ("data", Json.obj(("key0", "value0"))))),
      (
        RecordMetadata(
          HeaderMetadata(Json.obj(("key0", "value0")).some),
          PayloadMetadata(none, Json.obj(("key1", "value1")).some)),
        Json.obj(
          ("header", Json.obj(
            ("data", Json.obj(("key0", "value0"))))),
          ("payload", Json.obj(
            ("data", Json.obj(("key1", "value1"))))))))
  } {
    test(s"formatRecordMetadata reads $json") {
      json.validate[RecordMetadata] shouldEqual metadata.pure[JsResult]
    }
  }
}
