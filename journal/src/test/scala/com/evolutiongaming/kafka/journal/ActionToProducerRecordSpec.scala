package com.evolutiongaming.kafka.journal

import cats.data.NonEmptyList as Nel
import cats.syntax.all.*
import com.evolutiongaming.kafka.journal.ExpireAfter.implicits.*
import com.evolutiongaming.kafka.journal.conversions.{ActionToProducerRecord, ConsRecordToActionRecord, KafkaWrite}
import com.evolutiongaming.skafka.consumer.WithSize
import com.evolutiongaming.skafka.{TimestampAndType, TimestampType, TopicPartition}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import play.api.libs.json.Json

import java.time.Instant
import java.time.temporal.ChronoUnit
import scala.concurrent.duration.*
import scala.util.Try

import TestJsonCodec.instance

class ActionToProducerRecordSpec extends AnyFunSuite with Matchers {

  private val key1 = Key(id = "id", topic = "topic")

  private val timestamp = Instant.now().truncatedTo(ChronoUnit.MILLIS)

  private val partitionOffset = PartitionOffset.empty

  private val topicPartition = TopicPartition(key1.topic, partitionOffset.partition)

  private val origins = List(Origin("origin").some, none[Origin])

  private val versions = List(Version.current.some, none[Version])

  private val seqNrs = List(SeqNr.min, SeqNr.max)

  private val actionToProducerRecord = ActionToProducerRecord[Try]

  private val deletes = for {
    origin  <- origins
    version <- versions
    seqNr   <- seqNrs
  } yield {
    Action.Delete(key1, timestamp, seqNr.toDeleteTo, origin, version)
  }

  private val purges = for {
    origin  <- origins
    version <- versions
  } yield {
    Action.Purge(key1, timestamp, origin, version)
  }

  private val marks = for {
    origin  <- origins
    version <- versions
  } yield {
    Action.Mark(key1, timestamp, "id", origin, version)
  }

  private val metadata = List(HeaderMetadata.empty, HeaderMetadata(Json.obj(("key", "value")).some))

  private val payloads = {
    def binary(a: String) = PayloadBinaryFromStr(a)
    List(Payload.text("text").some, Payload.json(Json.obj(("key", "value"))).some, binary("bytes").some, none[Payload])
  }

  private val events = for {
    tags    <- List(Tags.empty, Tags("tag"))
    payload <- payloads
    seqNrs  <- List(Nel.of(SeqNr.min), Nel.of(SeqNr.max), Nel.of(SeqNr.unsafe(1), SeqNr.unsafe(2), SeqNr.unsafe(3)))
  } yield {
    for {
      seqNr <- seqNrs
    } yield {
      Event(seqNr, tags, payload)
    }
  }

  private val headers = List(Headers.empty, Headers(("key", "value")))

  private val consRecordToActionRecord = ConsRecordToActionRecord[Try]

  private val payloadMetadatas = for {
    expireAfter <- List(1.day.some, none)
    metadata    <- List(Json.obj(("key1", "value1")).some, none)
  } yield {
    PayloadMetadata(expireAfter.map { _.toExpireAfter }, metadata)
  }

  private val appends = {
    implicit val kafkaWrite = KafkaWrite.summon[Try, Payload]
    for {
      origin          <- origins
      version         <- versions
      metadata        <- metadata
      events          <- events
      headers         <- headers
      payloadMetadata <- payloadMetadatas
    } yield {
      Action
        .Append
        .of[Try, Payload](
          key       = key1,
          timestamp = timestamp,
          origin    = origin,
          version   = version,
          events    = Events(events, payloadMetadata),
          metadata  = metadata,
          headers   = headers,
        )
        .get
    }
  }

  for {
    actions <- List(appends, deletes, purges, marks)
    action  <- actions
  } {

    test(s"toProducerRecord & toActionRecord $action") {
      for {
        producerRecord <- actionToProducerRecord(action)
      } yield {
        val consRecord = ConsRecord(
          topicPartition   = topicPartition,
          offset           = partitionOffset.offset,
          timestampAndType = TimestampAndType(timestamp, TimestampType.Create).some,
          key              = producerRecord.key.map(bytes => WithSize(bytes, bytes.length)),
          value            = producerRecord.value.map(bytes => WithSize(bytes, bytes.length.toInt)),
          headers          = producerRecord.headers,
        )

        val record = ActionRecord(action, partitionOffset)

        consRecordToActionRecord(consRecord) shouldEqual record.some
      }
    }
  }
}
