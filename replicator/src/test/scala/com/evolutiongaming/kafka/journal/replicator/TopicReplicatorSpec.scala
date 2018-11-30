package com.evolutiongaming.kafka.journal.replicator

import java.time.Instant

import com.evolutiongaming.kafka.journal.KafkaConverters._
import com.evolutiongaming.kafka.journal.eventual.{ReplicatedJournal, TopicPointers}
import com.evolutiongaming.kafka.journal.replicator.TopicReplicator.Metrics.Measurements
import com.evolutiongaming.kafka.journal.{IO, _}
import com.evolutiongaming.nel.Nel
import com.evolutiongaming.skafka.consumer.{ConsumerRecord, ConsumerRecords, WithSize}
import com.evolutiongaming.skafka.{Bytes => _, _}
import org.scalatest.{Matchers, WordSpec}

import scala.util.control.NonFatal

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
      val io = topicReplicator.shutdown()
      val (result, _) = io.run(data)

      result shouldEqual Data(
        topics = List(topic),
        commits = List(Map(
          (topicPartitionOf(0), offsetAndMetadata(5)),
          (topicPartitionOf(1), offsetAndMetadata(5)))),
        stopped = true,
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
        metadata = Map(
          metadataOf("0-0", partition = 0, offset = 3),
          metadataOf("0-1", partition = 0, offset = 4),
          metadataOf("1-0", partition = 1, offset = 3),
          metadataOf("1-1", partition = 1, offset = 4)),
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
      val io = topicReplicator.shutdown()
      val (result, _) = io.run(data)

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
        stopped = true,
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
        metadata = Map(
          metadataOf("0-0", partition = 0, offset = 3),
          metadataOf("0-1", partition = 0, offset = 4),
          metadataOf("1-0", partition = 1, offset = 3),
          metadataOf("1-1", partition = 1, offset = 4)),
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
      val io = topicReplicator.shutdown()
      val (result, _) = io.run(data)

      result shouldEqual Data(
        topics = List(topic),
        commits = List(Map(
          (topicPartitionOf(0), offsetAndMetadata(10)),
          (topicPartitionOf(1), offsetAndMetadata(10)),
          (topicPartitionOf(2), offsetAndMetadata(10)))),
        stopped = true,
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
        metadata = Map(
          metadataOf("0-0", partition = 0, offset = 9),
          metadataOf("0-1", partition = 0, offset = 8),
          metadataOf("0-2", partition = 0, offset = 7),
          metadataOf("1-0", partition = 1, offset = 9),
          metadataOf("1-1", partition = 1, offset = 8),
          metadataOf("1-2", partition = 1, offset = 7),
          metadataOf("2-0", partition = 2, offset = 9),
          metadataOf("2-1", partition = 2, offset = 8),
          metadataOf("2-2", partition = 2, offset = 7)),
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
      val io = topicReplicator.shutdown()
      val (result, _) = io.run(data)

      result shouldEqual Data(
        topics = List(topic),
        commits = List(Map(
          (topicPartitionOf(0), offsetAndMetadata(11)),
          (topicPartitionOf(1), offsetAndMetadata(11)))),
        stopped = true,
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
        metadata = Map(
          metadataOf("0-0", partition = 0, offset = 10),
          metadataOf("0-1", partition = 0, offset = 9, deleteTo = Some(2)),
          metadataOf("0-2", partition = 0, offset = 7),
          metadataOf("1-0", partition = 1, offset = 10),
          metadataOf("1-1", partition = 1, offset = 9, deleteTo = Some(2)),
          metadataOf("1-2", partition = 1, offset = 7)),
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
      val io = topicReplicator.shutdown()
      val (result, _) = io.run(data)

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
        stopped = true,
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
        metadata = Map(
          metadataOf("0-0", partition = 0, offset = 11, deleteTo = Some(1)),
          metadataOf("0-1", partition = 0, offset = 12, deleteTo = Some(2)),
          metadataOf("0-2", partition = 0, offset = 7)),
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
      val io = topicReplicator.shutdown()
      val (result, _) = io.run(data)
      result shouldEqual Data(
        topics = List(topic),
        stopped = true,
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
      val io = topicReplicator.shutdown()
      val (result, _) = io.run(data)

      result shouldEqual Data(
        topics = List(topic),
        commits = List(Map(
          (topicPartitionOf(0), offsetAndMetadata(4)),
          (topicPartitionOf(1), offsetAndMetadata(4)),
          (topicPartitionOf(2), offsetAndMetadata(4)))),
        stopped = true,
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
        metadata = Map(
          metadataOf("0-0", partition = 0, offset = 2),
          metadataOf("0-1", partition = 0, offset = 3),
          metadataOf("1-0", partition = 1, offset = 2),
          metadataOf("1-1", partition = 1, offset = 3),
          metadataOf("2-1", partition = 2, offset = 3)),
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
    ReplicatedEvent(event, timestamp, partitionOffset, Some(origin))
  }

  private def appendOf(key: Key, seqNrs: Nel[Int]) = {
    val events = seqNrs.map { seqNr => Event(SeqNr(seqNr.toLong), Set(seqNr.toString)) }
    Action.Append(key, timestamp = timestamp, Some(origin), events)
  }

  private def markOf(key: Key) = {
    Action.Mark(key, timestamp, Some(origin), "id")
  }

  private def deleteOf(key: Key, to: Int) = {
    Action.Delete(key, timestamp, Some(origin), SeqNr(to.toLong))
  }
}

object TopicReplicatorSpec {

  val topic = "topic"

  val timestamp = Instant.now()

  val origin = Origin("origin")

  val replicationLatency: Long = 10

  val timestampAndType = TimestampAndType(timestamp, TimestampType.Create)

  def topicPartitionOf(partition: Partition) = TopicPartition(topic, partition)

  def offsetAndMetadata(offset: Offset) = OffsetAndMetadata(offset)

  def keyOf(id: Id) = Key(id = id, topic = topic)

  def metadataOf(id: Id, partition: Partition, offset: Offset, deleteTo: Option[Int] = None): (Key, Metadata) = {
    val deleteToSeqNr = deleteTo.flatMap(deleteTo => SeqNr.opt(deleteTo.toLong))
    val partitionOffset = PartitionOffset(partition = partition, offset = offset)
    val metadata = Metadata(partitionOffset, deleteToSeqNr)
    val key = keyOf(id = id)
    (key, metadata)
  }

  val consumer: KafkaConsumer[TestF] = new KafkaConsumer[TestF] {

    def subscribe(topic: Topic) = TestF { _.subscribe(topic) }

    def commit(offsets: Map[TopicPartition, OffsetAndMetadata]) = TestF { _.commit(offsets) }

    def poll() = TestF { _.poll }

    def close() = TestF { data => (data, ()) }
  }


  val journal: ReplicatedJournal[TestF] = new ReplicatedJournal[TestF] {

    def topics() = IO[TestF].iterable

    def pointers(topic: Topic) = {
      TestF { data => (data, data.pointers.getOrElse(topic, TopicPointers.Empty)) }
    }

    def append(key: Key, partitionOffset: PartitionOffset, timestamp: Instant, events: Nel[ReplicatedEvent]) = {
      TestF { _.append(key, partitionOffset, events) }
    }

    def delete(key: Key, partitionOffset: PartitionOffset, timestamp: Instant, deleteTo: SeqNr, origin: Option[Origin]) = {
      // TODO test origin
      TestF { _.delete(key, deleteTo, partitionOffset) }
    }

    def save(topic: Topic, pointers: TopicPointers, timestamp: Instant) = {
      TestF { _.save(topic, pointers) }
    }
  }


  val log: Log[TestF] = Log.empty[TestF](TestF.TestIOIO.unit)


  val stopRef: AtomicRef[Boolean, TestF] = new AtomicRef[Boolean, TestF] {

    def set(value: Boolean) = TestF { _.stop(value) }

    def get() = TestF { data => (data, data.stopped) }
  }


  sealed trait Metrics

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

  val metrics: TopicReplicator.Metrics[TestF] = new TopicReplicator.Metrics[TestF] {

    def append(events: Int, bytes: Int, measurements: Measurements) = {
      TestF {
        _ + Metrics.Append(
          partition = measurements.partition,
          latency = measurements.replicationLatency,
          events = events,
          records = measurements.records)
      }
    }

    def delete(measurements: Measurements) = {
      TestF {
        _ + Metrics.Delete(
          partition = measurements.partition,
          latency = measurements.replicationLatency,
          actions = measurements.records)
      }
    }

    def round(duration: Long, records: Int) = {
      TestF { _ + Metrics.Round(duration = duration, records = records) }
    }
  }

  val topicReplicator: TopicReplicator[TestF] = {
    val timestampReplicated = timestamp.plusMillis(replicationLatency)
    TopicReplicator(
      topic = topic,
      consumer = consumer,
      journal = journal,
      log = log,
      stopRef = stopRef,
      metrics = metrics,
      now = TestF { (_, timestampReplicated) })
  }


  // TODO create separate case class covering state of KafkaConsumer for testing
  final case class Data(
    topics: List[Topic] = Nil,
    commits: List[Map[TopicPartition, OffsetAndMetadata]] = Nil,
    records: List[ConsumerRecords[Id, Bytes]] = Nil,
    stopped: Boolean = false,
    pointers: Map[Topic, TopicPointers] = Map.empty,
    journal: Map[Key, List[ReplicatedEvent]] = Map.empty,
    metadata: Map[Key, Metadata] = Map.empty,
    metrics: List[Metrics] = Nil) { self =>

    def +(metrics: Metrics): (Data, Unit) = {
      val result = copy(metrics = metrics :: self.metrics)
      (result, ())
    }

    def subscribe(topic: Topic): (Data, Unit) = {
      val result = copy(topics = topic :: topics)
      (result, ())
    }

    def commit(offsets: Map[TopicPartition, OffsetAndMetadata]): (Data, Unit) = {
      val result = copy(commits = offsets :: commits)
      (result, ())
    }

    def stop(value: Boolean): (Data, Unit) = {
      (this, ())
    }

    def save(topic: Topic, pointers: TopicPointers): (Data, Unit) = {
      val updated = self.pointers.getOrElse(topic, TopicPointers.Empty) + pointers
      val result = copy(pointers = self.pointers.updated(topic, updated))
      (result, ())
    }

    def append(key: Key, partitionOffset: PartitionOffset, events: Nel[ReplicatedEvent]): (Data, Unit) = {

      val records = events.toList ++ self.journal.getOrElse(key, Nil)

      val deletedTo = self.metadata.get(key).flatMap(_.deleteTo)

      val metadata = Metadata(partitionOffset, deletedTo)

      val updated = copy(
        journal = journal.updated(key, records),
        metadata = self.metadata.updated(key, metadata))

      (updated, ())
    }

    def delete(key: Key, deleteTo: SeqNr, partitionOffset: PartitionOffset): (Data, Unit) = {

      def journal = self.journal.getOrElse(key, Nil)

      def delete(deleteTo: SeqNr) = journal.dropWhile(_.seqNr <= deleteTo)

      val deletedTo = self.metadata.get(key).flatMap(_.deleteTo)
      val result = {
        if (deletedTo.exists(_ >= deleteTo)) {
          self.metadata.get(key).fold(this) { metadata =>
            copy(metadata = self.metadata.updated(key, metadata.copy(offset = partitionOffset)))
          }
        } else {
          val records = delete(deleteTo)
          val result = records.headOption.flatMap(_.seqNr.prev) orElse journal.lastOption.map(_.seqNr)
          val metadata = Metadata(partitionOffset, deleteTo = result orElse deletedTo)
          copy(
            journal = self.journal.updated(key, records),
            metadata = self.metadata.updated(key, metadata))
        }
      }

      (result, ())
    }

    def poll: (Data, ConsumerRecords[Id, Bytes]) = {
      records match {
        case head :: tail => (copy(records = tail), head)
        case Nil          => (copy(stopped = true), ConsumerRecords(Map.empty))
      }
    }
  }

  final case class TestF[A](run: Data => (Data, A)) { self =>

    def map[B](ab: A => B): TestF[B] = {
      TestF { a => self.run(a) match { case (t, a) => (t, ab(a)) } }
    }

    def flatMap[B](afb: A => TestF[B]): TestF[B] = {
      TestF { a => self.run(a) match { case (b, a) => afb(a).run(b) } }
    }

    def flatMapFailure[B >: A](f: Throwable => TestF[B]): TestF[B] = {
      TestF { data =>
        try self.run(data) catch { case NonFatal(failure) => f(failure).run(data) }
      }
    }
  }

  object TestF {

    implicit val TestIOIO: IO[TestF] = new IO[TestF] {

      def pure[A](a: A) = TestF { data => (data, a) }

      def point[A](a: => A) = TestF { data => (data, a) }

      def effect[A](a: => A) = TestF { data => (data, a) }

      def fail[A](failure: Throwable) = throw failure

      def flatMap[A, B](fa: TestF[A])(afb: A => TestF[B]) = fa.flatMap(afb)

      def map[A, B](fa: TestF[A])(ab: A => B) = fa.map(ab)

      def foldWhile[S](s: S)(f: S => TestF[S], b: S => Boolean) = {
        def loop(s: S): TestF[S] = for {
          s <- f(s)
          s <- if (b(s)) loop(s) else pure(s)
        } yield s

        loop(s)
      }

      def flatMapFailure[A, B >: A](fa: TestF[A], f: Throwable => TestF[B]) = fa.flatMapFailure(f)

      def bracket[A, B](acquire: TestF[A])(release: A => TestF[Unit])(use: A => TestF[B]) = {
        for {
          a <- acquire
          b <- try use(a) finally { release(a) }
        } yield b
      }
    }
  }

  final case class Metadata(offset: PartitionOffset, deleteTo: Option[SeqNr])
}
