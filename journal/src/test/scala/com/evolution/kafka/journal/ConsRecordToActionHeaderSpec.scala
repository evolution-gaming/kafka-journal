package com.evolution.kafka.journal

import cats.syntax.all.*
import com.evolution.kafka.journal.conversions.ConsRecordToActionHeader
import com.evolutiongaming.skafka.consumer.ConsumerRecord
import com.evolutiongaming.skafka.{Header, Offset, Partition, TopicPartition}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.nio.charset.StandardCharsets
import scala.util.{Failure, Try}

class ConsRecordToActionHeaderSpec extends AnyFunSuite with Matchers {
  import ConsRecordToActionHeaderSpec.*

  List(
    ("default", JsonCodec.default[Try]),
    ("streaming", JsonCodec.streaming[Try]),
  ).foreach {
    case (name, jsonCodec) =>
      val consRecordToActionHeader = {
        implicit val jsonCodec1: JsonCodec[Try] = jsonCodec
        ConsRecordToActionHeader[Try]
      }

      def parse(record: ConsumerRecord[String, Unit]): Try[Option[ActionHeader]] = {
        consRecordToActionHeader(record).value
      }

      test(s"$name: find the action header among other headers") {
        val header = ActionHeader.Purge(origin = Origin("origin").some, version = Version("1.0.0").some)
        val json = """{"purge":{"origin":"origin","version":"1.0.0"}}"""
        val record = recordOf(Header("other", bytesOf("value")), Header(ActionHeader.key, bytesOf(json)))
        parse(record) shouldEqual header.some.pure[Try]
      }

      test(s"$name: fall back to play-json for a header jsoniter rejects") {
        // jsoniter does not accept a leading byte order mark, play-json (Jackson) does
        val json = "﻿" + """{"purge":{}}"""
        val record = recordOf(Header(ActionHeader.key, bytesOf(json)))
        parse(record) shouldEqual ActionHeader.Purge(none, none).some.pure[Try]
      }

      test(s"$name: None without the action header") {
        parse(recordOf()) shouldEqual none[ActionHeader].pure[Try]
        parse(recordOf(Header("other", bytesOf("value")))) shouldEqual none[ActionHeader].pure[Try]
      }

      test(s"$name: None for an unknown action") {
        val record = recordOf(Header(ActionHeader.key, bytesOf("""{"unknown":{}}""")))
        parse(record) shouldEqual none[ActionHeader].pure[Try]
      }

      test(s"$name: JournalError for a malformed header") {
        val notJson = recordOf(Header(ActionHeader.key, bytesOf("not json")))
        parse(notJson) should matchPattern { case Failure(_: JournalError) => }
        val invalidAppend = recordOf(Header(ActionHeader.key, bytesOf("""{"append":{"range":"invalid"}}""")))
        parse(invalidAppend) should matchPattern { case Failure(_: JournalError) => }
      }
  }
}

object ConsRecordToActionHeaderSpec {

  def bytesOf(value: String): Array[Byte] = value.getBytes(StandardCharsets.UTF_8)

  def recordOf(headers: Header*): ConsumerRecord[String, Unit] = {
    ConsumerRecord(
      topicPartition = TopicPartition(topic = "topic", partition = Partition.min),
      offset = Offset.min,
      timestampAndType = none,
      headers = headers.toList,
    )
  }
}
