package com.evolutiongaming.kafka.journal.replicator

import java.time.Instant

import cats.effect._
import cats.implicits._
import cats.{Applicative, Monoid}
import com.evolutiongaming.kafka.journal.KafkaConverters._
import com.evolutiongaming.kafka.journal._
import com.evolutiongaming.kafka.journal.eventual.{ReplicatedJournal, TopicPointers}
import com.evolutiongaming.kafka.journal.replicator.TopicReplicator.Metrics.Measurements
import com.evolutiongaming.nel.Nel
import com.evolutiongaming.skafka.consumer.{ConsumerRecord, ConsumerRecords, WithSize}
import com.evolutiongaming.skafka.{Bytes => _, Metadata => _, _}
import org.scalatest.{Matchers, WordSpec}
import play.api.libs.json.Json

import scala.annotation.tailrec
import scala.concurrent.ExecutionContext
import scala.util.control.{NoStackTrace, NonFatal}

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
            append("0", Nel(1)),
            append("1", Nel(1, 2)),
            append("0", Nel(2)),
            append("1", Nel(3)))

          val records = for {
            (action, idx) <- actions.zipWithIndex
          } yield {
            val offset = idx + 1l
            consumerRecordOf(action, topicPartition, offset)
          }
          (topicPartition, records)
        }
        ConsumerRecords(records.toMap) // TODO use ConsumerRecordsOf
      }

      val data = Data(records = List(records))

      val (result, _) = topicReplicator.run(data)

      result shouldEqual Data(
        topics = List(topic),
        commits = List(Map(
          (topicPartitionOf(0), offsetAndMetadata(5)),
          (topicPartitionOf(1), offsetAndMetadata(5)))),
        stopAfter = Some(0),
        pointers = Map((topic, TopicPointers(Map((0, 4l), (1, 4l))))),
        journal = Map(
          (keyOf("0-0"), List(
            replicated(seqNr = 1, partition = 0, offset = 1),
            replicated(seqNr = 2, partition = 0, offset = 3))),
          (keyOf("0-1"), List(
            replicated(seqNr = 1, partition = 0, offset = 2),
            replicated(seqNr = 2, partition = 0, offset = 2),
            replicated(seqNr = 3, partition = 0, offset = 4))),
          (keyOf("1-0"), List(
            replicated(seqNr = 1, partition = 1, offset = 1),
            replicated(seqNr = 2, partition = 1, offset = 3))),
          (keyOf("1-1"), List(
            replicated(seqNr = 1, partition = 1, offset = 2),
            replicated(seqNr = 2, partition = 1, offset = 2),
            replicated(seqNr = 3, partition = 1, offset = 4)))),
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
            append("0", Nel(1)),
            append("1", Nel(1, 2)),
            append("0", Nel(2)),
            append("1", Nel(3)))

          for {
            (record, idx) <- kafkaRecords.zipWithIndex
          } yield {
            val offset = idx + 1l
            val consumerRecord = consumerRecordOf(record, topicPartition, offset)
            ConsumerRecords(Map((topicPartition, List(consumerRecord))))
          }
        }
      } yield record

      val data = Data(records = records)
      val (result, _) = topicReplicator.run(data)

      result shouldEqual Data(
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
            replicated(seqNr = 2, partition = 0, offset = 3),
            replicated(seqNr = 1, partition = 0, offset = 1))),
          (keyOf("0-1"), List(
            replicated(seqNr = 3, partition = 0, offset = 4),
            replicated(seqNr = 1, partition = 0, offset = 2),
            replicated(seqNr = 2, partition = 0, offset = 2))),
          (keyOf("1-0"), List(
            replicated(seqNr = 2, partition = 1, offset = 3),
            replicated(seqNr = 1, partition = 1, offset = 1))),
          (keyOf("1-1"), List(
            replicated(seqNr = 3, partition = 1, offset = 4),
            replicated(seqNr = 1, partition = 1, offset = 2),
            replicated(seqNr = 2, partition = 1, offset = 2)))),
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
            append("0", Nel(1)),
            mark("3"),
            append("1", Nel(1, 2)),
            mark("2"),
            append("0", Nel(2)),
            mark("1"),
            append("2", Nel(1, 2, 3)),
            append("1", Nel(3)),
            mark("0"))

          val records = for {
            (record, idx) <- kafkaRecords.zipWithIndex
          } yield {
            val offset = idx + 1l
            consumerRecordOf(record, topicPartition, offset)
          }
          (topicPartition, records)
        }
        ConsumerRecords(records.toMap)
      }

      val data = Data(records = List(records))
      val (result, _) = topicReplicator.run(data)

      result shouldEqual Data(
        topics = List(topic),
        commits = List(Map(
          (topicPartitionOf(0), offsetAndMetadata(10)),
          (topicPartitionOf(1), offsetAndMetadata(10)),
          (topicPartitionOf(2), offsetAndMetadata(10)))),
        stopAfter = Some(0),
        pointers = Map((topic, TopicPointers(Map((0, 9l), (1, 9l), (2, 9l))))),
        journal = Map(
          (keyOf("0-0"), List(
            replicated(seqNr = 1, partition = 0, offset = 1),
            replicated(seqNr = 2, partition = 0, offset = 5))),
          (keyOf("0-1"), List(
            replicated(seqNr = 1, partition = 0, offset = 3),
            replicated(seqNr = 2, partition = 0, offset = 3),
            replicated(seqNr = 3, partition = 0, offset = 8))),
          (keyOf("0-2"), List(
            replicated(seqNr = 1, partition = 0, offset = 7),
            replicated(seqNr = 2, partition = 0, offset = 7),
            replicated(seqNr = 3, partition = 0, offset = 7))),
          (keyOf("1-0"), List(
            replicated(seqNr = 1, partition = 1, offset = 1),
            replicated(seqNr = 2, partition = 1, offset = 5))),
          (keyOf("1-1"), List(
            replicated(seqNr = 1, partition = 1, offset = 3),
            replicated(seqNr = 2, partition = 1, offset = 3),
            replicated(seqNr = 3, partition = 1, offset = 8))),
          (keyOf("1-2"), List(
            replicated(seqNr = 1, partition = 1, offset = 7),
            replicated(seqNr = 2, partition = 1, offset = 7),
            replicated(seqNr = 3, partition = 1, offset = 7))),
          (keyOf("2-0"), List(
            replicated(seqNr = 1, partition = 2, offset = 1),
            replicated(seqNr = 2, partition = 2, offset = 5))),
          (keyOf("2-1"), List(
            replicated(seqNr = 1, partition = 2, offset = 3),
            replicated(seqNr = 2, partition = 2, offset = 3),
            replicated(seqNr = 3, partition = 2, offset = 8))),
          (keyOf("2-2"), List(
            replicated(seqNr = 1, partition = 2, offset = 7),
            replicated(seqNr = 2, partition = 2, offset = 7),
            replicated(seqNr = 3, partition = 2, offset = 7)))),
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
            append("0", Nel(1)),
            mark("3"),
            append("1", Nel(1, 2)),
            mark("2"),
            append("0", Nel(2)),
            mark("1"),
            append("2", Nel(1, 2, 3)),
            append("1", Nel(3)),
            delete("1", 2),
            mark("0"))

          val records = for {
            (record, idx) <- kafkaRecords.zipWithIndex
          } yield {
            val offset = idx + 1l
            consumerRecordOf(record, topicPartition, offset)
          }
          (topicPartition, records)
        }
        ConsumerRecords(records.toMap)
      }

      val data = Data(records = List(records))
      val (result, _) = topicReplicator.run(data)

      result shouldEqual Data(
        topics = List(topic),
        commits = List(Map(
          (topicPartitionOf(0), offsetAndMetadata(11)),
          (topicPartitionOf(1), offsetAndMetadata(11)))),
        stopAfter = Some(0),
        pointers = Map((topic, TopicPointers(Map((0, 10l), (1, 10l))))),
        journal = Map(
          (keyOf("0-0"), List(
            replicated(seqNr = 1, partition = 0, offset = 1),
            replicated(seqNr = 2, partition = 0, offset = 5))),
          (keyOf("0-1"), List(
            replicated(seqNr = 3, partition = 0, offset = 8))),
          (keyOf("0-2"), List(
            replicated(seqNr = 1, partition = 0, offset = 7),
            replicated(seqNr = 2, partition = 0, offset = 7),
            replicated(seqNr = 3, partition = 0, offset = 7))),
          (keyOf("1-0"), List(
            replicated(seqNr = 1, partition = 1, offset = 1),
            replicated(seqNr = 2, partition = 1, offset = 5))),
          (keyOf("1-1"), List(
            replicated(seqNr = 3, partition = 1, offset = 8))),
          (keyOf("1-2"), List(
            replicated(seqNr = 1, partition = 1, offset = 7),
            replicated(seqNr = 2, partition = 1, offset = 7),
            replicated(seqNr = 3, partition = 1, offset = 7)))),
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
            append("0", Nel(1)),
            mark("3"),
            append("1", Nel(1, 2)),
            mark("2"),
            append("0", Nel(2)),
            mark("1"),
            append("2", Nel(1, 2, 3)),
            append("1", Nel(3)),
            delete("1", 2),
            delete("0", 5),
            delete("0", 6),
            delete("1", 2))

          for {
            (record, idx) <- kafkaRecords.zipWithIndex
          } yield {
            val offset = idx + 1l
            val consumerRecord = consumerRecordOf(record, topicPartition, offset)
            ConsumerRecords(Map((topicPartition, List(consumerRecord))))
          }
        }
      } yield result

      val data = Data(records = records)
      val (result, _) = topicReplicator.run(data)

      result shouldEqual Data(
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
            replicated(seqNr = 3, partition = 0, offset = 8),
            replicated(seqNr = 1, partition = 0, offset = 3),
            replicated(seqNr = 2, partition = 0, offset = 3))),
          (keyOf("0-2"), List(
            replicated(seqNr = 1, partition = 0, offset = 7),
            replicated(seqNr = 2, partition = 0, offset = 7),
            replicated(seqNr = 3, partition = 0, offset = 7)))),
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
      val data = Data(pointers = pointers)
      val (result, _) = topicReplicator.run(data)
      result shouldEqual Data(
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
            append("0", Nel(1)),
            append("1", Nel(1, 2)),
            append("0", Nel(2)),
            append("1", Nel(3)))

          val records = for {
            (action, offset) <- actions.zipWithIndex
          } yield {
            consumerRecordOf(action, topicPartition, offset.toLong)
          }
          (topicPartition, records)
        }
        ConsumerRecords(records.toMap)
      }

      val data = Data(
        records = List(records),
        pointers = Map((topic, TopicPointers(Map((0, 0l), (1, 1l), (2, 2l))))))
      val (result, _) = topicReplicator.run(data)

      result shouldEqual Data(
        topics = List(topic),
        commits = List(Map(
          (topicPartitionOf(0), offsetAndMetadata(4)),
          (topicPartitionOf(1), offsetAndMetadata(4)),
          (topicPartitionOf(2), offsetAndMetadata(4)))),
        stopAfter = Some(0),
        pointers = Map((topic, TopicPointers(Map((0, 3l), (1, 3l), (2, 3l))))),
        journal = Map(
          (keyOf("0-0"), List(
            replicated(seqNr = 2, partition = 0, offset = 2))),
          (keyOf("0-1"), List(
            replicated(seqNr = 1, partition = 0, offset = 1),
            replicated(seqNr = 2, partition = 0, offset = 1),
            replicated(seqNr = 3, partition = 0, offset = 3))),
          (keyOf("1-0"), List(
            replicated(seqNr = 2, partition = 1, offset = 2))),
          (keyOf("1-1"), List(
            replicated(seqNr = 3, partition = 1, offset = 3))),
          (keyOf("2-1"), List(
            replicated(seqNr = 3, partition = 2, offset = 3)))),
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

    val producerRecord = action.toProducerRecord
    ConsumerRecord(
      topicPartition = topicPartition,
      offset = offset,
      timestampAndType = Some(timestampAndType),
      key = producerRecord.key.map(WithSize(_)),
      value = producerRecord.value.map(WithSize(_)),
      headers = producerRecord.headers)
  }

  private def replicated(seqNr: Int, partition: Partition, offset: Offset) = {
    val partitionOffset = PartitionOffset(partition = partition, offset = offset)
    val event = Event(SeqNr(seqNr.toLong), Set(seqNr.toString))
    ReplicatedEvent(event, timestamp, partitionOffset, Some(origin), metadata)
  }

  private def appendOf(key: Key, seqNrs: Nel[Int]) = {
    val events = seqNrs.map { seqNr => Event(SeqNr(seqNr.toLong), Set(seqNr.toString)) }
    Action.Append(key, timestamp = timestamp, Some(origin), events, metadata)
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

  val replicationLatency: Long = 10

  val timestampAndType = TimestampAndType(timestamp, TimestampType.Create)

  def topicPartitionOf(partition: Partition) = TopicPartition(topic, partition)

  def offsetAndMetadata(offset: Offset) = OffsetAndMetadata(offset)

  def keyOf(id: Id) = Key(id = id, topic = topic)

  def headOf(id: Id, partition: Partition, offset: Offset, deleteTo: Option[Int] = None): (Key, Head) = {
    val deleteToSeqNr = deleteTo.flatMap(deleteTo => SeqNr.opt(deleteTo.toLong))
    val partitionOffset = PartitionOffset(partition = partition, offset = offset)
    val head = Head(partitionOffset, deleteToSeqNr)
    val key = keyOf(id = id)
    (key, head)
  }


  sealed abstract class Metrics extends Product

  object Metrics {

    final case class Append(
      partition: Partition,
      latency: Long = replicationLatency,
      events: Int,
      records: Int) extends Metrics

    final case class Delete(
      partition: Partition,
      latency: Long = replicationLatency,
      actions: Int) extends Metrics

    final case class Round(duration: Long = 0, records: Int) extends Metrics
  }


  val topicReplicator: DataF[Unit] = {
    val millis = timestamp.plusMillis(replicationLatency).toEpochMilli
    implicit val clock = ClockOf[DataF](millis)
    TopicReplicator.of[DataF](topic, TopicReplicator.StopRef[DataF], DataF.ConsumerDataF)
  }

  final case class Data(
    topics: List[Topic] = Nil,
    commits: List[Map[TopicPartition, OffsetAndMetadata]] = Nil,
    records: List[ConsumerRecords[Id, Bytes]] = Nil,
    stopAfter: Option[Int] = None,
    pointers: Map[Topic, TopicPointers] = Map.empty,
    journal: Map[Key, List[ReplicatedEvent]] = Map.empty,
    heads: Map[Key, Head] = Map.empty,
    metrics: List[Metrics] = Nil) { self =>

    def +(metrics: Metrics): (Data, Unit) = {
      val result = copy(metrics = metrics :: self.metrics)
      (result, ())
    }

    def subscribe(topic: Topic): Data = {
      copy(topics = topic :: topics)
    }

    def commit(offsets: Map[TopicPartition, OffsetAndMetadata]): Data = {
      copy(commits = offsets :: commits)
    }

    def stop: Data = copy(stopAfter = Some(1))

    def save(topic: Topic, pointers: TopicPointers): Data = {
      val updated = self.pointers.getOrElse(topic, TopicPointers.Empty) + pointers
      copy(pointers = self.pointers.updated(topic, updated))
    }

    def append(key: Key, partitionOffset: PartitionOffset, events: Nel[ReplicatedEvent]): (Data, Unit) = {

      val records = events.toList ++ self.journal.getOrElse(key, Nil)

      val deleteTo = self.heads.get(key).flatMap(_.deleteTo)

      val head = Head(partitionOffset, deleteTo)

      val updated = copy(
        journal = journal.updated(key, records),
        heads = self.heads.updated(key, head))

      (updated, ())
    }

    def delete(key: Key, deleteTo: SeqNr, partitionOffset: PartitionOffset): Data = {

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
        val head = Head(partitionOffset, deleteTo = result orElse deleteTo1)
        copy(
          journal = self.journal.updated(key, records),
          heads = self.heads.updated(key, head))
      }
    }

    def poll: (Data, ConsumerRecords[Id, Bytes]) = {
      records match {
        case head :: tail => (copy(records = tail), head)
        case Nil          => (copy(stopAfter = Some(0)), ConsumerRecords(Map.empty))
      }
    }
  }


  // TODO switch to StateT
  final case class DataF[A](run: Data => (Data, A))

  object DataF {

    implicit def monoidDataF[A : Monoid]: Monoid[DataF[A]] = Applicative.monoid[DataF, A]


    implicit val ConcurrentDataF: Concurrent[DataF] = new Concurrent[DataF] {

      def start[A](fa: DataF[A]) = {
        DataF { s =>
          val (s1, a) = fa.run(s)
          val fiber = new Fiber[DataF, A] {
            def cancel = DataF { s => (s, ()) }
            def join = DataF { s => (s, a) }
          }
          (s1, fiber)
        }
      }

      def racePair[A, B](fa: DataF[A], fb: DataF[B]) = throw NotImplemented

      def async[A](k: (Either[Throwable, A] => Unit) => Unit) = throw NotImplemented

      def asyncF[A](k: (Either[Throwable, A] => Unit) => DataF[Unit]) = throw NotImplemented

      def suspend[A](thunk: => DataF[A]) = thunk

      def bracketCase[A, B](acquire: DataF[A])(use: A => DataF[B])(release: (A, ExitCase[Throwable]) => DataF[Unit]) = {
        for {
          a <- acquire
          b <- handleErrorWith(use(a)) { e =>
            for {
              _ <- release(a, ExitCase.error(e))
            } yield raiseError[B](e)
          }
        } yield b
      }

      def flatMap[A, B](fa: DataF[A])(f: A => DataF[B]) = {
        DataF[B] { s =>
          val (s1, a) = fa.run(s)
          f(a).run(s1)
        }
      }

      def tailRecM[A, B](a: A)(f: A => DataF[Either[A, B]]) = {

        @tailrec
        def apply(s: Data, a: A): (Data, B) = {
          val (s1, b) = f(a).run(s)
          b match {
            case Right(b) => (s1, b)
            case Left(b)  => apply(s1, b)
          }
        }

        DataF { s => apply(s, a) }
      }

      def raiseError[A](e: Throwable) = throw e

      def handleErrorWith[A](fa: DataF[A])(f: Throwable => DataF[A]) = {
        DataF { s =>
          try fa.run(s) catch {
            case NonFatal(e) => f(e).run(s)
          }
        }
      }

      def pure[A](a: A) = DataF { s => (s, a) }
    }


    implicit val ContextShiftDataF: ContextShift[DataF] = new ContextShift[DataF] {

      def shift = ().pure[DataF]

      def evalOn[A](ec: ExecutionContext)(fa: DataF[A]) = fa
    }


    implicit val ReplicatedJournalDataF: ReplicatedJournal[DataF] = new ReplicatedJournal[DataF] {

      def topics = Iterable.empty[Topic].pure[DataF]

      def pointers(topic: Topic) = {
        DataF { data => (data, data.pointers.getOrElse(topic, TopicPointers.Empty)) }
      }

      def append(key: Key, partitionOffset: PartitionOffset, timestamp: Instant, events: Nel[ReplicatedEvent]) = {
        DataF { _.append(key, partitionOffset, events) }
      }

      def delete(key: Key, partitionOffset: PartitionOffset, timestamp: Instant, deleteTo: SeqNr, origin: Option[Origin]) = {
        // TODO test origin
        DataF { s => (s.delete(key, deleteTo, partitionOffset), ()) }
      }

      def save(topic: Topic, pointers: TopicPointers, timestamp: Instant) = {
        DataF { s => (s.save(topic, pointers), ()) }
      }
    }


    implicit val LogDataF: Log[DataF] = Log.empty[DataF]


    implicit val StopRefDataF: TopicReplicator.StopRef[DataF] = new TopicReplicator.StopRef[DataF] {

      def set = DataF { s => (s, ()) }

      def get = DataF { s =>
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


    implicit val ConsumerDataF: TopicReplicator.Consumer[DataF] = new TopicReplicator.Consumer[DataF] {

      def subscribe(topic: Topic) = DataF { s => (s.subscribe(topic), ()) }

      def commit(offsets: Map[TopicPartition, OffsetAndMetadata]) = DataF { s => (s.commit(offsets), ()) }

      def poll = DataF { _.poll }

      def assignment = Set.empty[TopicPartition].pure[DataF]
    }


    implicit val ParDataF: Par[DataF] = Par.sequential[DataF]


    implicit val MetricsDataF: TopicReplicator.Metrics[DataF] = new TopicReplicator.Metrics[DataF] {

      def append(events: Int, bytes: Int, measurements: Measurements) = {
        DataF {
          _ + Metrics.Append(
            partition = measurements.partition,
            latency = measurements.replicationLatency,
            events = events,
            records = measurements.records)
        }
      }

      def delete(measurements: Measurements) = {
        DataF {
          _ + Metrics.Delete(
            partition = measurements.partition,
            latency = measurements.replicationLatency,
            actions = measurements.records)
        }
      }

      def round(duration: Long, records: Int) = {
        DataF { _ + Metrics.Round(duration = duration, records = records) }
      }
    }
  }

  final case class Head(offset: PartitionOffset, deleteTo: Option[SeqNr])

  case object NotImplemented extends RuntimeException with NoStackTrace
}
