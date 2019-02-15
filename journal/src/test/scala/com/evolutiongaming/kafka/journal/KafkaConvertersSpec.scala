package com.evolutiongaming.kafka.journal

import java.time.Instant
import java.time.temporal.ChronoUnit

import cats.implicits._
import com.evolutiongaming.kafka.journal.KafkaConverters._
import com.evolutiongaming.nel.Nel
import com.evolutiongaming.skafka.consumer.{ConsumerRecord, WithSize}
import com.evolutiongaming.skafka.{TimestampAndType, TimestampType, TopicPartition}
import org.scalatest.{FunSuite, Matchers}
import play.api.libs.json.Json

class KafkaConvertersSpec extends FunSuite with Matchers {

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

  private val payloads = List(
    Payload.text("text").some,
    Payload.json(Json.obj(("key", "value"))).some,
    Payload.binary("bytes").some,
    none[Payload])

  private val events = for {
    tags    <- List(Tags.Empty, Tags("tag"))
    payload <- payloads
    seqNrs  <- List(
      Nel(SeqNr.Min),
      Nel(SeqNr.Max),
      Nel(SeqNr(1), SeqNr(2), SeqNr(3)))
  } yield {
    for {
      seqNr <- seqNrs
    } yield {
      Event(seqNr, tags, payload)
    }
  }

  private val headers = List(Headers.Empty, Headers(("key", "value")))

  private val appends = for {
    origin   <- origins
    metadata <- metadata
    events   <- events
    headers  <- headers
  } yield {
    Action.Append(key1, timestamp, origin, events, metadata, headers)
  }

  for {
    actions <- List(appends, deletes, marks)
    action <- actions
  } {

    test(s"toProducerRecord & toActionRecord $action") {
      val producerRecord = action.toProducerRecord

      val consumerRecord = ConsumerRecord[Id, Bytes](
        topicPartition = topicPartition,
        offset = partitionOffset.offset,
        timestampAndType = Some(TimestampAndType(timestamp, TimestampType.Create)),
        key = producerRecord.key.map(bytes => WithSize(bytes, bytes.length)),
        value = producerRecord.value.map(bytes => WithSize(bytes, bytes.length)),
        headers = producerRecord.headers)

      val record = ActionRecord(action, partitionOffset)

      consumerRecord.toActionRecord shouldEqual record.some
    }
  }
}
