package com.evolutiongaming.kafka.journal

import java.time.Instant
import java.time.temporal.ChronoUnit

import cats.implicits._
import cats.data.{NonEmptyList => Nel}
import com.evolutiongaming.kafka.journal.Conversion.implicits._
import com.evolutiongaming.kafka.journal.KafkaConversions._
import com.evolutiongaming.skafka.consumer.{ConsumerRecord, WithSize}
import com.evolutiongaming.skafka.producer.ProducerRecord
import com.evolutiongaming.skafka.{TimestampAndType, TimestampType, TopicPartition}
import org.scalatest.{FunSuite, Matchers}
import play.api.libs.json.Json
import scodec.bits.ByteVector

import scala.util.Try

class KafkaConversionsSpec extends FunSuite with Matchers {

  private val key1 = Key(id = "id", topic = "topic")

  private val timestamp = Instant.now().truncatedTo(ChronoUnit.MILLIS)

  private val partitionOffset = PartitionOffset.Empty

  private val topicPartition = TopicPartition(key1.topic, partitionOffset.partition)

  private val origins = List(Origin("origin").some, none[Origin])

  private val seqNrs = List(SeqNr.Min, SeqNr.Max)

  private val deletes = for {
    origin <- origins
    seqNr  <- seqNrs
  } yield {
    Action.Delete(key1, timestamp, seqNr, origin)
  }

  private val marks = for {
    origin <- origins
  } yield {
    Action.Mark(key1, timestamp, "id", origin)
  }

  private val metadata = List(
    Metadata.Empty,
    Metadata(Json.obj(("key", "value")).some))

  private val payloads = {
    def binary(a: String) = PayloadBinaryFromStr(a)
    List(
      Payload.text("text").some,
      Payload.json(Json.obj(("key", "value"))).some,
      binary("bytes").some,
      none[Payload])
  }

  private val events = for {
    tags    <- List(Tags.Empty, Tags("tag"))
    payload <- payloads
    seqNrs  <- List(
      Nel.of(SeqNr.Min),
      Nel.of(SeqNr.Max),
      Nel.of(SeqNr(1), SeqNr(2), SeqNr(3)))
  } yield {
    for {
      seqNr <- seqNrs
    } yield {
      Event(seqNr, tags, payload)
    }
  }

  private val headers = List(Headers.Empty, Headers(("key", "value")))

  private val appends = {
    implicit val eventsToPayload = PayloadAndType.eventsToPayload[Try]
    for {
      origin   <- origins
      metadata <- metadata
      events   <- events
      headers  <- headers
    } yield {
      Action.Append.of[Try](key1, timestamp, origin, events, metadata, headers).get
    }
  }

  for {
    actions <- List(appends, deletes, marks)
    action <- actions
  } {

    test(s"toProducerRecord & toActionRecord $action") {
      for {
        producerRecord <- action.convert[Try, ProducerRecord[Id, ByteVector]]
      } yield {
        val consumerRecord = ConsumerRecord[Id, ByteVector](
          topicPartition = topicPartition,
          offset = partitionOffset.offset,
          timestampAndType = Some(TimestampAndType(timestamp, TimestampType.Create)),
          key = producerRecord.key.map(bytes => WithSize(bytes, bytes.length)),
          value = producerRecord.value.map(bytes => WithSize(bytes, bytes.length.toInt)),
          headers = producerRecord.headers)

        val record = ActionRecord(action, partitionOffset)

        implicit val consumerRecordToActionHeaderTry = consumerRecordToActionHeader[Try]
        consumerRecordToActionRecord[Try].apply(consumerRecord).value shouldEqual record.some
      }
    }
  }
}
