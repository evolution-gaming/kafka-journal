package com.evolutiongaming.kafka.journal.replicator

import java.time.Instant

import cats.data.{NonEmptyList => Nel}
import cats.effect._
import cats.implicits._
import cats.{Applicative, Monoid, Parallel}
import com.evolutiongaming.catshelper.ClockHelper._
import com.evolutiongaming.catshelper.{FromTry, Log}
import com.evolutiongaming.kafka.journal.Conversion.implicits._
import com.evolutiongaming.kafka.journal.KafkaConversions._
import com.evolutiongaming.kafka.journal._
import com.evolutiongaming.kafka.journal.eventual.{ReplicatedJournal, TopicPointers}
import com.evolutiongaming.kafka.journal.replicator.TopicReplicator.Metrics.Measurements
import com.evolutiongaming.kafka.journal.util.ConcurrentOf
import com.evolutiongaming.skafka.consumer.{ConsumerRecord, ConsumerRecords, WithSize}
import com.evolutiongaming.skafka.producer.ProducerRecord
import com.evolutiongaming.skafka.{Bytes => _, Metadata => _, Header => _, _}
import org.scalatest.{Matchers, WordSpec}
import play.api.libs.json.Json
import scodec.bits.ByteVector

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import scala.util.Try
import scala.util.control.NoStackTrace

class TopicReplicatorSpec extends WordSpec with Matchers {
  import TopicReplicatorSpec._

  "TopicReplicator" should {

    "replicate appends" in {
      val records = {
        val records = for {
          partition <- 0 to 1
        } yield {

          def keyOf(id: String) = Key(id = s"$partition-$id", topic = topic)

          def append(id: String, seqNrs: Nel[Int]) = {
            val key = keyOf(id)
            appendOf(key, seqNrs)
          }

          val topicPartition = topicPartitionOf(partition)

          val actions = List(
            append("0", Nel.of(1)),
            append("1", Nel.of(1, 2)),
            append("0", Nel.of(2)),
            append("1", Nel.of(3)))

          val records = for {
            (action, idx) <- actions.zipWithIndex
          } yield {
            val offset = idx + 1l
            consumerRecordOf(action, topicPartition, offset)
          }
          (topicPartition, Nel.fromListUnsafe(records))
        }
        ConsumerRecords(records.toMap) // TODO use ConsumerRecordsOf
      }

      val data = State(records = List(records))

      val (result, _) = topicReplicator.run(data)

      result shouldEqual State(
        topics = List(topic),
        commits = List(Map(
          (topicPartitionOf(0), offsetAndMetadata(5)),
          (topicPartitionOf(1), offsetAndMetadata(5)))),
        stopAfter = Some(0),
        pointers = Map((topic, TopicPointers(Map((0, 4l), (1, 4l))))),
        journal = Map(
          (keyOf("0-0"), List(
            record(seqNr = 1, partition = 0, offset = 1),
            record(seqNr = 2, partition = 0, offset = 3))),
          (keyOf("0-1"), List(
            record(seqNr = 1, partition = 0, offset = 2),
            record(seqNr = 2, partition = 0, offset = 2),
            record(seqNr = 3, partition = 0, offset = 4))),
          (keyOf("1-0"), List(
            record(seqNr = 1, partition = 1, offset = 1),
            record(seqNr = 2, partition = 1, offset = 3))),
          (keyOf("1-1"), List(
            record(seqNr = 1, partition = 1, offset = 2),
            record(seqNr = 2, partition = 1, offset = 2),
            record(seqNr = 3, partition = 1, offset = 4)))),
        heads = Map(
          headOf("0-0", partition = 0, offset = 3),
          headOf("0-1", partition = 0, offset = 4),
          headOf("1-0", partition = 1, offset = 3),
          headOf("1-1", partition = 1, offset = 4)),
        metrics = List(
          Metrics.Round(records = 8),
          Metrics.Append(partition = 0, events = 2, records = 2),
          Metrics.Append(partition = 1, events = 3, records = 2),
          Metrics.Append(partition = 0, events = 3, records = 2),
          Metrics.Append(partition = 1, events = 2, records = 2)))
    }

    "replicate appends of many polls" in {
      val records = for {
        partition <- (0 to 1).toList
        record <- {
          def keyOf(id: String) = Key(id = s"$partition-$id", topic = topic)

          def append(id: String, seqNrs: Nel[Int]) = {
            val key = keyOf(id)
            appendOf(key, seqNrs)
          }

          val topicPartition = topicPartitionOf(partition)

          val kafkaRecords = List(
            append("0", Nel.of(1)),
            append("1", Nel.of(1, 2)),
            append("0", Nel.of(2)),
            append("1", Nel.of(3)))

          for {
            (record, idx) <- kafkaRecords.zipWithIndex
          } yield {
            val offset = idx + 1l
            val consumerRecord = consumerRecordOf(record, topicPartition, offset)
            ConsumerRecords(Map((topicPartition, Nel.of(consumerRecord))))
          }
        }
      } yield record

      val data = State(records = records)
      val (result, _) = topicReplicator.run(data)

      result shouldEqual State(
        topics = List(topic),
        commits = List(
          Map((topicPartitionOf(1), offsetAndMetadata(5))),
          Map((topicPartitionOf(1), offsetAndMetadata(4))),
          Map((topicPartitionOf(1), offsetAndMetadata(3))),
          Map((topicPartitionOf(1), offsetAndMetadata(2))),
          Map((topicPartitionOf(0), offsetAndMetadata(5))),
          Map((topicPartitionOf(0), offsetAndMetadata(4))),
          Map((topicPartitionOf(0), offsetAndMetadata(3))),
          Map((topicPartitionOf(0), offsetAndMetadata(2)))),
        stopAfter = Some(0),
        pointers = Map((topic, TopicPointers(Map((0, 4l), (1, 4l))))),
        journal = Map(
          (keyOf("0-0"), List(
            record(seqNr = 2, partition = 0, offset = 3),
            record(seqNr = 1, partition = 0, offset = 1))),
          (keyOf("0-1"), List(
            record(seqNr = 3, partition = 0, offset = 4),
            record(seqNr = 1, partition = 0, offset = 2),
            record(seqNr = 2, partition = 0, offset = 2))),
          (keyOf("1-0"), List(
            record(seqNr = 2, partition = 1, offset = 3),
            record(seqNr = 1, partition = 1, offset = 1))),
          (keyOf("1-1"), List(
            record(seqNr = 3, partition = 1, offset = 4),
            record(seqNr = 1, partition = 1, offset = 2),
            record(seqNr = 2, partition = 1, offset = 2)))),
        heads = Map(
          headOf("0-0", partition = 0, offset = 3),
          headOf("0-1", partition = 0, offset = 4),
          headOf("1-0", partition = 1, offset = 3),
          headOf("1-1", partition = 1, offset = 4)),
        metrics = List(
          Metrics.Round(records = 1),
          Metrics.Append(partition = 1, events = 1, records = 1),
          Metrics.Round(records = 1),
          Metrics.Append(partition = 1, events = 1, records = 1),
          Metrics.Round(records = 1),
          Metrics.Append(partition = 1, events = 2, records = 1),
          Metrics.Round(records = 1),
          Metrics.Append(partition = 1, events = 1, records = 1),
          Metrics.Round(records = 1),
          Metrics.Append(partition = 0, events = 1, records = 1),
          Metrics.Round(records = 1),
          Metrics.Append(partition = 0, events = 1, records = 1),
          Metrics.Round(records = 1),
          Metrics.Append(partition = 0, events = 2, records = 1),
          Metrics.Round(records = 1),
          Metrics.Append(partition = 0, events = 1, records = 1)))
    }

    "replicate appends and ignore marks" in {
      val records = {
        val records = for {
          partition <- 0 to 2
        } yield {

          def keyOf(id: String) = Key(id = s"$partition-$id", topic = topic)

          def append(id: String, seqNrs: Nel[Int]) = {
            val key = keyOf(id)
            appendOf(key, seqNrs)
          }

          def mark(id: String) = {
            val key = keyOf(id)
            markOf(key)
          }

          val topicPartition = topicPartitionOf(partition)

          val kafkaRecords = List(
            append("0", Nel.of(1)),
            mark("3"),
            append("1", Nel.of(1, 2)),
            mark("2"),
            append("0", Nel.of(2)),
            mark("1"),
            append("2", Nel.of(1, 2, 3)),
            append("1", Nel.of(3)),
            mark("0"))

          val records = for {
            (record, idx) <- kafkaRecords.zipWithIndex
          } yield {
            val offset = idx + 1l
            consumerRecordOf(record, topicPartition, offset)
          }
          (topicPartition, Nel.fromListUnsafe(records))
        }
        ConsumerRecords(records.toMap)
      }

      val data = State(records = List(records))
      val (result, _) = topicReplicator.run(data)

      result shouldEqual State(
        topics = List(topic),
        commits = List(Map(
          (topicPartitionOf(0), offsetAndMetadata(10)),
          (topicPartitionOf(1), offsetAndMetadata(10)),
          (topicPartitionOf(2), offsetAndMetadata(10)))),
        stopAfter = Some(0),
        pointers = Map((topic, TopicPointers(Map((0, 9l), (1, 9l), (2, 9l))))),
        journal = Map(
          (keyOf("0-0"), List(
            record(seqNr = 1, partition = 0, offset = 1),
            record(seqNr = 2, partition = 0, offset = 5))),
          (keyOf("0-1"), List(
            record(seqNr = 1, partition = 0, offset = 3),
            record(seqNr = 2, partition = 0, offset = 3),
            record(seqNr = 3, partition = 0, offset = 8))),
          (keyOf("0-2"), List(
            record(seqNr = 1, partition = 0, offset = 7),
            record(seqNr = 2, partition = 0, offset = 7),
            record(seqNr = 3, partition = 0, offset = 7))),
          (keyOf("1-0"), List(
            record(seqNr = 1, partition = 1, offset = 1),
            record(seqNr = 2, partition = 1, offset = 5))),
          (keyOf("1-1"), List(
            record(seqNr = 1, partition = 1, offset = 3),
            record(seqNr = 2, partition = 1, offset = 3),
            record(seqNr = 3, partition = 1, offset = 8))),
          (keyOf("1-2"), List(
            record(seqNr = 1, partition = 1, offset = 7),
            record(seqNr = 2, partition = 1, offset = 7),
            record(seqNr = 3, partition = 1, offset = 7))),
          (keyOf("2-0"), List(
            record(seqNr = 1, partition = 2, offset = 1),
            record(seqNr = 2, partition = 2, offset = 5))),
          (keyOf("2-1"), List(
            record(seqNr = 1, partition = 2, offset = 3),
            record(seqNr = 2, partition = 2, offset = 3),
            record(seqNr = 3, partition = 2, offset = 8))),
          (keyOf("2-2"), List(
            record(seqNr = 1, partition = 2, offset = 7),
            record(seqNr = 2, partition = 2, offset = 7),
            record(seqNr = 3, partition = 2, offset = 7)))),
        heads = Map(
          headOf("0-0", partition = 0, offset = 9),
          headOf("0-1", partition = 0, offset = 8),
          headOf("0-2", partition = 0, offset = 7),
          headOf("1-0", partition = 1, offset = 9),
          headOf("1-1", partition = 1, offset = 8),
          headOf("1-2", partition = 1, offset = 7),
          headOf("2-0", partition = 2, offset = 9),
          headOf("2-1", partition = 2, offset = 8),
          headOf("2-2", partition = 2, offset = 7)),
        metrics = List(
          Metrics.Round(records = 27),
          Metrics.Append(partition = 0, events = 3, records = 1),
          Metrics.Append(partition = 1, events = 2, records = 2),
          Metrics.Append(partition = 1, events = 3, records = 2),
          Metrics.Append(partition = 2, events = 3, records = 1),
          Metrics.Append(partition = 0, events = 3, records = 2),
          Metrics.Append(partition = 2, events = 2, records = 2),
          Metrics.Append(partition = 2, events = 3, records = 2),
          Metrics.Append(partition = 0, events = 2, records = 2),
          Metrics.Append(partition = 1, events = 3, records = 1)))
    }

    "replicate appends and deletes" in {
      val records = {
        val records = for {
          partition <- 0 to 1
        } yield {

          def keyOf(id: String) = Key(id = s"$partition-$id", topic = topic)

          def append(id: String, seqNrs: Nel[Int]) = {
            val key = keyOf(id)
            appendOf(key, seqNrs)
          }

          def mark(id: String) = {
            val key = keyOf(id)
            markOf(key)
          }

          def delete(id: String, to: Int) = {
            val key = keyOf(id)
            deleteOf(key, to)
          }

          val topicPartition = topicPartitionOf(partition)

          val kafkaRecords = List(
            append("0", Nel.of(1)),
            mark("3"),
            append("1", Nel.of(1, 2)),
            mark("2"),
            append("0", Nel.of(2)),
            mark("1"),
            append("2", Nel.of(1, 2, 3)),
            append("1", Nel.of(3)),
            delete("1", 2),
            mark("0"))

          val records = for {
            (record, idx) <- kafkaRecords.zipWithIndex
          } yield {
            val offset = idx + 1l
            consumerRecordOf(record, topicPartition, offset)
          }
          (topicPartition, Nel.fromListUnsafe(records))
        }
        ConsumerRecords(records.toMap)
      }

      val data = State(records = List(records))
      val (result, _) = topicReplicator.run(data)

      result shouldEqual State(
        topics = List(topic),
        commits = List(Map(
          (topicPartitionOf(0), offsetAndMetadata(11)),
          (topicPartitionOf(1), offsetAndMetadata(11)))),
        stopAfter = Some(0),
        pointers = Map((topic, TopicPointers(Map((0, 10l), (1, 10l))))),
        journal = Map(
          (keyOf("0-0"), List(
            record(seqNr = 1, partition = 0, offset = 1),
            record(seqNr = 2, partition = 0, offset = 5))),
          (keyOf("0-1"), List(
            record(seqNr = 3, partition = 0, offset = 8))),
          (keyOf("0-2"), List(
            record(seqNr = 1, partition = 0, offset = 7),
            record(seqNr = 2, partition = 0, offset = 7),
            record(seqNr = 3, partition = 0, offset = 7))),
          (keyOf("1-0"), List(
            record(seqNr = 1, partition = 1, offset = 1),
            record(seqNr = 2, partition = 1, offset = 5))),
          (keyOf("1-1"), List(
            record(seqNr = 3, partition = 1, offset = 8))),
          (keyOf("1-2"), List(
            record(seqNr = 1, partition = 1, offset = 7),
            record(seqNr = 2, partition = 1, offset = 7),
            record(seqNr = 3, partition = 1, offset = 7)))),
        heads = Map(
          headOf("0-0", partition = 0, offset = 10),
          headOf("0-1", partition = 0, offset = 9, deleteTo = Some(2)),
          headOf("0-2", partition = 0, offset = 7),
          headOf("1-0", partition = 1, offset = 10),
          headOf("1-1", partition = 1, offset = 9, deleteTo = Some(2)),
          headOf("1-2", partition = 1, offset = 7)),
        metrics = List(
          Metrics.Round(records = 20),
          Metrics.Append(partition = 0, events = 3, records = 1),
          Metrics.Append(partition = 1, events = 2, records = 2),
          Metrics.Delete(partition = 1, actions = 1),
          Metrics.Append(partition = 1, events = 3, records = 2),
          Metrics.Delete(partition = 0, actions = 1),
          Metrics.Append(partition = 0, events = 3, records = 2),
          Metrics.Append(partition = 0, events = 2, records = 2),
          Metrics.Append(partition = 1, events = 3, records = 1)))
    }

    "replicate appends and deletes of many polls" in {
      val records = for {
        partition <- (0 to 0).toList
        result <- {

          def keyOf(id: String) = Key(id = s"$partition-$id", topic = topic)

          def append(id: String, seqNrs: Nel[Int]) = {
            val key = keyOf(id)
            appendOf(key, seqNrs)
          }

          def mark(id: String) = {
            val key = keyOf(id)
            markOf(key)
          }

          def delete(id: String, to: Int) = {
            val key = keyOf(id)
            deleteOf(key, to)
          }

          val topicPartition = topicPartitionOf(partition)

          val kafkaRecords = List(
            append("0", Nel.of(1)),
            mark("3"),
            append("1", Nel.of(1, 2)),
            mark("2"),
            append("0", Nel.of(2)),
            mark("1"),
            append("2", Nel.of(1, 2, 3)),
            append("1", Nel.of(3)),
            delete("1", 2),
            delete("0", 5),
            delete("0", 6),
            delete("1", 2))

          for {
            (record, idx) <- kafkaRecords.zipWithIndex
          } yield {
            val offset = idx + 1l
            val consumerRecord = consumerRecordOf(record, topicPartition, offset)
            ConsumerRecords(Map((topicPartition, Nel.of(consumerRecord))))
          }
        }
      } yield result

      val data = State(records = records)
      val (result, _) = topicReplicator.run(data)

      result shouldEqual State(
        topics = List(topic),
        commits = List(
          Map((topicPartitionOf(0), offsetAndMetadata(13))),
          Map((topicPartitionOf(0), offsetAndMetadata(12))),
          Map((topicPartitionOf(0), offsetAndMetadata(11))),
          Map((topicPartitionOf(0), offsetAndMetadata(10))),
          Map((topicPartitionOf(0), offsetAndMetadata(9))),
          Map((topicPartitionOf(0), offsetAndMetadata(8))),
          Map((topicPartitionOf(0), offsetAndMetadata(7))),
          Map((topicPartitionOf(0), offsetAndMetadata(6))),
          Map((topicPartitionOf(0), offsetAndMetadata(5))),
          Map((topicPartitionOf(0), offsetAndMetadata(4))),
          Map((topicPartitionOf(0), offsetAndMetadata(3))),
          Map((topicPartitionOf(0), offsetAndMetadata(2)))),
        stopAfter = Some(0),
        pointers = Map(
          (topic, TopicPointers(Map((0, 12l))))),
        journal = Map(
          (keyOf("0-0"), Nil),
          (keyOf("0-1"), List(
            record(seqNr = 3, partition = 0, offset = 8),
            record(seqNr = 1, partition = 0, offset = 3),
            record(seqNr = 2, partition = 0, offset = 3))),
          (keyOf("0-2"), List(
            record(seqNr = 1, partition = 0, offset = 7),
            record(seqNr = 2, partition = 0, offset = 7),
            record(seqNr = 3, partition = 0, offset = 7)))),
        heads = Map(
          headOf("0-0", partition = 0, offset = 11, deleteTo = Some(1)),
          headOf("0-1", partition = 0, offset = 12, deleteTo = Some(2)),
          headOf("0-2", partition = 0, offset = 7)),
        metrics = List(
          Metrics.Round(records = 1),
          Metrics.Delete(partition = 0, actions = 1),
          Metrics.Round(records = 1),
          Metrics.Delete(partition = 0, actions = 1),
          Metrics.Round(records = 1),
          Metrics.Delete(partition = 0, actions = 1),
          Metrics.Round(records = 1),
          Metrics.Delete(partition = 0, actions = 1),
          Metrics.Round(records = 1),
          Metrics.Append(partition = 0, events = 1, records = 1),
          Metrics.Round(records = 1),
          Metrics.Append(partition = 0, events = 3, records = 1),
          Metrics.Round(records = 1),
          Metrics.Round(records = 1),
          Metrics.Append(partition = 0, events = 1, records = 1),
          Metrics.Round(records = 1),
          Metrics.Round(records = 1),
          Metrics.Append(partition = 0, events = 2, records = 1),
          Metrics.Round(records = 1),
          Metrics.Round(records = 1),
          Metrics.Append(partition = 0, events = 1, records = 1)))
    }

    "consume since replicated offset" in {
      val pointers = Map((topic, TopicPointers(Map((0, 1l), (2, 2l)))))
      val data = State(pointers = pointers)
      val (result, _) = topicReplicator.run(data)
      result shouldEqual State(
        topics = List(topic),
        stopAfter = Some(0),
        pointers = pointers)
    }

    "ignore already replicated data" in {
      val records = {
        val records = for {
          partition <- 0 to 2
        } yield {

          def keyOf(id: String) = Key(id = s"$partition-$id", topic = topic)

          def append(id: String, seqNrs: Nel[Int]) = {
            val key = keyOf(id)
            appendOf(key, seqNrs)
          }

          val topicPartition = topicPartitionOf(partition)

          val actions = List(
            append("0", Nel.of(1)),
            append("1", Nel.of(1, 2)),
            append("0", Nel.of(2)),
            append("1", Nel.of(3)))

          val records = for {
            (action, offset) <- actions.zipWithIndex
          } yield {
            consumerRecordOf(action, topicPartition, offset.toLong)
          }
          (topicPartition, Nel.fromListUnsafe(records))
        }
        ConsumerRecords(records.toMap)
      }

      val data = State(
        records = List(records),
        pointers = Map((topic, TopicPointers(Map((0, 0l), (1, 1l), (2, 2l))))))
      val (result, _) = topicReplicator.run(data)

      result shouldEqual State(
        topics = List(topic),
        commits = List(Map(
          (topicPartitionOf(0), offsetAndMetadata(4)),
          (topicPartitionOf(1), offsetAndMetadata(4)),
          (topicPartitionOf(2), offsetAndMetadata(4)))),
        stopAfter = Some(0),
        pointers = Map((topic, TopicPointers(Map((0, 3l), (1, 3l), (2, 3l))))),
        journal = Map(
          (keyOf("0-0"), List(
            record(seqNr = 2, partition = 0, offset = 2))),
          (keyOf("0-1"), List(
            record(seqNr = 1, partition = 0, offset = 1),
            record(seqNr = 2, partition = 0, offset = 1),
            record(seqNr = 3, partition = 0, offset = 3))),
          (keyOf("1-0"), List(
            record(seqNr = 2, partition = 1, offset = 2))),
          (keyOf("1-1"), List(
            record(seqNr = 3, partition = 1, offset = 3))),
          (keyOf("2-1"), List(
            record(seqNr = 3, partition = 2, offset = 3)))),
        heads = Map(
          headOf("0-0", partition = 0, offset = 2),
          headOf("0-1", partition = 0, offset = 3),
          headOf("1-0", partition = 1, offset = 2),
          headOf("1-1", partition = 1, offset = 3),
          headOf("2-1", partition = 2, offset = 3)),
        metrics = List(
          Metrics.Round(records = 12),
          Metrics.Append(partition = 1, events = 1, records = 1),
          Metrics.Append(partition = 1, events = 1, records = 1),
          Metrics.Append(partition = 0, events = 3, records = 2),
          Metrics.Append(partition = 2, events = 1, records = 1),
          Metrics.Append(partition = 0, events = 1, records = 1)))
    }
  }

  private def consumerRecordOf(
    action: Action,
    topicPartition: TopicPartition,
    offset: Offset) = {

    val producerRecord = action.convert[Try, ProducerRecord[Id, ByteVector]].get
    ConsumerRecord(
      topicPartition = topicPartition,
      offset = offset,
      timestampAndType = Some(timestampAndType),
      key = producerRecord.key.map(WithSize(_)),
      value = producerRecord.value.map(WithSize(_)),
      headers = producerRecord.headers)
  }

  private def record(seqNr: Int, partition: Partition, offset: Offset) = {
    val partitionOffset = PartitionOffset(partition = partition, offset = offset)
    val event = Event(SeqNr(seqNr.toLong), Set(seqNr.toString))
    EventRecord(event, timestamp, partitionOffset, Some(origin), metadata, headers)
  }

  private def appendOf(key: Key, seqNrs: Nel[Int]) = {
    implicit val eventsToPayload = PayloadAndType.eventsToPayload[Try]
    val events = seqNrs.map { seqNr => Event(SeqNr(seqNr.toLong), Set(seqNr.toString)) }
    Action.Append.of[Try](key, timestamp = timestamp, Some(origin), events, metadata, headers).get
  }

  private def markOf(key: Key) = {
    Action.Mark(key, timestamp, "id", Some(origin))
  }

  private def deleteOf(key: Key, to: Int) = {
    Action.Delete(key, timestamp, SeqNr(to.toLong), Some(origin))
  }
}

object TopicReplicatorSpec {

  val topic = "topic"

  val timestamp = Instant.now()

  val origin = Origin("origin")

  val metadata = Metadata(Json.obj(("key", "value")).some)

  val headers = Headers(("key", "value"))

  val replicationLatency: FiniteDuration = 10.millis

  val timestampAndType = TimestampAndType(timestamp, TimestampType.Create)

  def topicPartitionOf(partition: Partition) = TopicPartition(topic, partition)

  def offsetAndMetadata(offset: Offset) = OffsetAndMetadata(offset)

  def keyOf(id: Id) = Key(id = id, topic = topic)

  def headOf(
    id: Id,
    partition: Partition,
    offset: Offset,
    deleteTo: Option[Int] = None
  ): (Key, Head) = {
    val deleteToSeqNr = deleteTo.flatMap(deleteTo => SeqNr.opt(deleteTo.toLong))
    val partitionOffset = PartitionOffset(partition = partition, offset = offset)
    val head = Head(partitionOffset, deleteToSeqNr, origin.some)
    val key = keyOf(id = id)
    (key, head)
  }


  sealed abstract class Metrics extends Product

  object Metrics {

    final case class Append(
      partition: Partition,
      latency: FiniteDuration = replicationLatency,
      events: Int,
      records: Int) extends Metrics

    final case class Delete(
      partition: Partition,
      latency: FiniteDuration = replicationLatency,
      actions: Int) extends Metrics

    final case class Round(duration: FiniteDuration = 0.millis, records: Int) extends Metrics
  }


  implicit def monoidDataF[A : Monoid]: Monoid[StateT[A]] = Applicative.monoid[StateT, A]


  implicit val contextShift: ContextShift[StateT] = new ContextShift[StateT] {

    def shift = ().pure[StateT]

    def evalOn[A](ec: ExecutionContext)(fa: StateT[A]) = fa
  }


  implicit val replicatedJournal: ReplicatedJournal[StateT] = new ReplicatedJournal[StateT] {

    def topics = Iterable.empty[Topic].pure[StateT]

    def pointers(topic: Topic) = {
      StateT { state => (state, state.pointers.getOrElse(topic, TopicPointers.Empty)) }
    }

    def append(key: Key, partitionOffset: PartitionOffset, timestamp: Instant, events: Nel[EventRecord]) = {
      StateT { _.append(key, partitionOffset, events) }
    }

    def delete(key: Key, partitionOffset: PartitionOffset, timestamp: Instant, deleteTo: SeqNr, origin: Option[Origin]) = {
      StateT { state => (state.delete(key, deleteTo, partitionOffset, origin), ()) }
    }

    def save(topic: Topic, pointers: TopicPointers, timestamp: Instant) = {
      StateT { state => (state.save(topic, pointers), ()) }
    }
  }


  implicit val log: Log[StateT] = Log.empty[StateT]


  implicit val stopRef: TopicReplicator.StopRef[StateT] = new TopicReplicator.StopRef[StateT] {

    def set = StateT { s => (s, ()) }

    def get = StateT { s =>
      s.stopAfter.fold((s, false)) { stopped =>
        if (stopped <= 0) {
          (s, true)
        } else {
          val s1 = s.copy(stopAfter = Some(stopped - 1))
          (s1, false)
        }
      }
    }
  }


  implicit val consumer: TopicReplicator.Consumer[StateT] = new TopicReplicator.Consumer[StateT] {

    def subscribe(topic: Topic) = StateT { s => (s.subscribe(topic), ()) }

    def commit(offsets: Map[TopicPartition, OffsetAndMetadata]) = StateT { s => (s.commit(offsets), ()) }

    def poll = StateT { _.poll }

    def assignment = Set.empty[TopicPartition].pure[StateT]
  }


  implicit val parallel = Parallel.identity[StateT]


  implicit val metrics: TopicReplicator.Metrics[StateT] = new TopicReplicator.Metrics[StateT] {

    def append(events: Int, bytes: Long, measurements: Measurements) = {
      StateT {
        _ + Metrics.Append(
          partition = measurements.partition,
          latency = measurements.replicationLatency,
          events = events,
          records = measurements.records)
      }
    }

    def delete(measurements: Measurements) = {
      StateT {
        _ + Metrics.Delete(
          partition = measurements.partition,
          latency = measurements.replicationLatency,
          actions = measurements.records)
      }
    }

    def round(duration: FiniteDuration, records: Int) = {
      StateT { _ + Metrics.Round(duration = duration, records = records) }
    }
  }

  
  val topicReplicator: StateT[Unit] = {
    val millis = timestamp.toEpochMilli + replicationLatency.toMillis
    implicit val concurrent = ConcurrentOf.fromMonad[StateT]
    implicit val fromTry = FromTry.lift[StateT]
    implicit val fromAttempt = FromAttempt.lift[StateT]
    implicit val fromJsResult = FromJsResult.lift[StateT]
    
    implicit val clock = Clock.const[StateT](nanos = 0, millis = millis)
    implicit val payloadToEvents = PayloadAndType.payloadToEvents[StateT]

    TopicReplicator.of[StateT](topic, TopicReplicator.StopRef[StateT], consumer, 1.second)
  }


  final case class State(
    topics: List[Topic] = Nil,
    commits: List[Map[TopicPartition, OffsetAndMetadata]] = Nil,
    records: List[ConsumerRecords[Id, ByteVector]] = Nil,
    stopAfter: Option[Int] = None,
    pointers: Map[Topic, TopicPointers] = Map.empty,
    journal: Map[Key, List[EventRecord]] = Map.empty,
    heads: Map[Key, Head] = Map.empty,
    metrics: List[Metrics] = Nil) { self =>

    def +(metrics: Metrics): (State, Unit) = {
      val result = copy(metrics = metrics :: self.metrics)
      (result, ())
    }

    def subscribe(topic: Topic): State = {
      copy(topics = topic :: topics)
    }

    def commit(offsets: Map[TopicPartition, OffsetAndMetadata]): State = {
      copy(commits = offsets :: commits)
    }

    def stop: State = copy(stopAfter = Some(1))

    def save(topic: Topic, pointers: TopicPointers): State = {
      val updated = self.pointers.getOrElse(topic, TopicPointers.Empty) + pointers
      copy(pointers = self.pointers.updated(topic, updated))
    }

    def append(key: Key, partitionOffset: PartitionOffset, events: Nel[EventRecord]): (State, Unit) = {

      val records = events.toList ++ self.journal.getOrElse(key, Nil)

      val deleteTo = self.heads.get(key).flatMap(_.deleteTo)

      val head = Head(partitionOffset, deleteTo, events.last.origin)

      val updated = copy(
        journal = journal.updated(key, records),
        heads = self.heads.updated(key, head))

      (updated, ())
    }

    def delete(key: Key, deleteTo: SeqNr, partitionOffset: PartitionOffset, origin: Option[Origin]): State = {

      def journal = self.journal.getOrElse(key, Nil)

      def delete(deleteTo: SeqNr) = journal.dropWhile(_.seqNr <= deleteTo)

      val deleteTo1 = self.heads.get(key).flatMap(_.deleteTo)
      if (deleteTo1.exists(_ >= deleteTo)) {
        self.heads.get(key).fold(this) { head =>
          copy(heads = self.heads.updated(key, head.copy(offset = partitionOffset)))
        }
      } else {
        val records = delete(deleteTo)
        val result = records.headOption.flatMap(_.seqNr.prev) orElse journal.lastOption.map(_.seqNr)
        val head = Head(partitionOffset, deleteTo = result orElse deleteTo1, origin = origin)
        copy(
          journal = self.journal.updated(key, records),
          heads = self.heads.updated(key, head))
      }
    }

    def poll: (State, ConsumerRecords[Id, ByteVector]) = {
      records match {
        case head :: tail => (copy(records = tail), head)
        case Nil          => (copy(stopAfter = Some(0)), ConsumerRecords(Map.empty))
      }
    }
  }


  type StateT[A] = cats.data.StateT[cats.Id, State, A]

  object StateT {
    def apply[A](f: State => (State, A)): StateT[A] = cats.data.StateT[cats.Id, State, A](f)
  }


  final case class Head(offset: PartitionOffset, deleteTo: Option[SeqNr], origin: Option[Origin])

  case object NotImplemented extends RuntimeException with NoStackTrace
}
