package com.evolutiongaming.kafka.journal

import java.time.Instant
import java.time.temporal.ChronoUnit

import cats.implicits._
import cats.data.{NonEmptyList => Nel}
import com.evolutiongaming.kafka.journal.conversions.{ActionToProducerRecord, ConsRecordToActionRecord, EventsToPayload}
import com.evolutiongaming.kafka.journal.ExpireAfter.implicits._
import com.evolutiongaming.skafka.consumer.WithSize
import com.evolutiongaming.skafka.{TimestampAndType, TimestampType, TopicPartition}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import play.api.libs.json.Json

import scala.concurrent.duration._
import scala.util.Try

class ActionToProducerRecordSpec extends AnyFunSuite with Matchers {

  private val key1 = Key(id = "id", topic = "topic")

  private val timestamp = Instant.now().truncatedTo(ChronoUnit.MILLIS)

  private val partitionOffset = PartitionOffset.empty

  private val topicPartition = TopicPartition(key1.topic, partitionOffset.partition)

  private val origins = List(Origin("origin").some, none[Origin])

  private val seqNrs = List(SeqNr.min, SeqNr.max)

  private val actionToProducerRecord = ActionToProducerRecord[Try]

  private val deletes = for {
    origin <- origins
    seqNr  <- seqNrs
  } yield {
    Action.Delete(key1, timestamp, seqNr.toDeleteTo, origin)
  }

  private val purges = for {
    origin <- origins
  } yield {
    Action.Purge(key1, timestamp, origin)
  }

  private val marks = for {
    origin <- origins
  } yield {
    Action.Mark(key1, timestamp, "id", origin)
  }

  private val metadata = List(
    HeaderMetadata.empty,
    HeaderMetadata(Json.obj(("key", "value")).some))

  private val payloads = {
    def binary(a: String) = PayloadBinaryFromStr(a)
    List(
      Payload.text("text").some,
      Payload.json(Json.obj(("key", "value"))).some,
      binary("bytes").some,
      none[Payload])
  }

  private val events = for {
    tags    <- List(Tags.empty, Tags("tag"))
    payload <- payloads
    seqNrs  <- List(
      Nel.of(SeqNr.min),
      Nel.of(SeqNr.max),
      Nel.of(SeqNr.unsafe(1), SeqNr.unsafe(2), SeqNr.unsafe(3)))
  } yield {
    for {
      seqNr <- seqNrs
    } yield {
      Event(seqNr, tags, payload)
    }
  }

  private val headers = List(Headers.empty, Headers(("key", "value")))

  private val consRecordToActionRecord = ConsRecordToActionRecord[Try]

  private val appends = {
    implicit val eventsToPayload = EventsToPayload[Try]
    for {
      origin      <- origins
      metadata    <- metadata
      events      <- events
      headers     <- headers
      expireAfter <- List(1.day.some, none)
    } yield {
      Action.Append.of[Try](
        key = key1,
        timestamp = timestamp,
        origin = origin,
        events = Events(events, PayloadMetadata.empty/*TODO expiry: pass metadata*/),
        expireAfter = expireAfter.map { _.toExpireAfter },
        metadata = metadata,
        headers = headers).get
    }
  }

  for {
    actions <- List(appends, deletes, purges, marks)
    action <- actions
  } {

    test(s"toProducerRecord & toActionRecord $action") {
      for {
        producerRecord <- actionToProducerRecord(action)
      } yield {
        val consRecord = ConsRecord(
          topicPartition = topicPartition,
          offset = partitionOffset.offset,
          timestampAndType = TimestampAndType(timestamp, TimestampType.Create).some,
          key = producerRecord.key.map(bytes => WithSize(bytes, bytes.length)),
          value = producerRecord.value.map(bytes => WithSize(bytes, bytes.length.toInt)),
          headers = producerRecord.headers)

        val record = ActionRecord(action, partitionOffset)

        consRecordToActionRecord(consRecord) shouldEqual record.some
      }
    }
  }
}
