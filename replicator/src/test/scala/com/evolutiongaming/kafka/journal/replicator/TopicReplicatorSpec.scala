package com.evolutiongaming.kafka.journal.replicator

import java.time.Instant

import cats.data.{NonEmptyList => Nel, NonEmptyMap => Nem}
import cats.effect._
import cats.implicits._
import cats.{Applicative, Id, Monoid, Parallel}
import com.evolutiongaming.catshelper.ClockHelper._
import com.evolutiongaming.catshelper.{FromTry, Log}
import com.evolutiongaming.kafka.journal.{ConsRecords, _}
import com.evolutiongaming.kafka.journal.conversions.{ActionToProducerRecord, ConsRecordToActionRecord, EventsToPayload, PayloadToEvents}
import com.evolutiongaming.kafka.journal.eventual.{ReplicatedJournal, ReplicatedKeyJournal, ReplicatedTopicJournal, TopicPointers}
import com.evolutiongaming.kafka.journal.replicator.TopicReplicator.Metrics.Measurements
import com.evolutiongaming.kafka.journal.util.{ConcurrentOf, Fail}
import com.evolutiongaming.kafka.journal.ExpireAfter.implicits._
import com.evolutiongaming.catshelper.DataHelper._
import com.evolutiongaming.sstream.Stream
import com.evolutiongaming.skafka.consumer.{ConsumerRecord, ConsumerRecords, RebalanceListener, WithSize}
import com.evolutiongaming.skafka.{Bytes => _, Header => _, Metadata => _, _}
import com.evolutiongaming.smetrics.MeasureDuration
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import play.api.libs.json.Json

import scala.concurrent.duration._
import scala.util.Try
import scala.util.control.NoStackTrace

class TopicReplicatorSpec extends AnyWordSpec with Matchers {
  import TopicReplicatorSpec._

  "TopicReplicator" should {

    "replicate appends" in {
      val records = {
        val records = for {
          partition      <- 0 to 1
          keyOf           = (id: String) => Key(id = s"$partition-$id", topic = topic)
          append          = (id: String, seqNrs: Nel[Int]) => appendOf(keyOf(id), seqNrs)
          topicPartition  = topicPartitionOf(partition)
          actions         = List(
            append("0", Nel.of(1)),
            append("1", Nel.of(1, 2)),
            append("0", Nel.of(2)),
            append("1", Nel.of(3)))
          (action, idx)  <- actions.zipWithIndex
          offset          = idx + 1L
        } yield {
          consumerRecordOf(action, topicPartition, offset)
        }
        ConsumerRecordsOf(records.toList)
      }

      val data = State(records = List(records))

      val (result, _) = topicReplicator.run(data)

      result shouldEqual State(
        topics = List(topic),
        commits = List(Nem.of(
          (0, 5),
          (1, 5))),
        pointers = Map((topic, Map((0, 4L), (1, 4L)))),
        journal = Map(
          ("0-0", List(
            record(seqNr = 1, partition = 0, offset = 1),
            record(seqNr = 2, partition = 0, offset = 3))),
          ("0-1", List(
            record(seqNr = 1, partition = 0, offset = 2),
            record(seqNr = 2, partition = 0, offset = 2),
            record(seqNr = 3, partition = 0, offset = 4))),
          ("1-0", List(
            record(seqNr = 1, partition = 1, offset = 1),
            record(seqNr = 2, partition = 1, offset = 3))),
          ("1-1", List(
            record(seqNr = 1, partition = 1, offset = 2),
            record(seqNr = 2, partition = 1, offset = 2),
            record(seqNr = 3, partition = 1, offset = 4)))),
        metaJournal = Map(
          metaJournalOf("0-0", partition = 0, offset = 3),
          metaJournalOf("0-1", partition = 0, offset = 4),
          metaJournalOf("1-0", partition = 1, offset = 3),
          metaJournalOf("1-1", partition = 1, offset = 4)),
        metrics = List(
          Metrics.Round(records = 8),
          Metrics.Append(partition = 1, events = 3, records = 2),
          Metrics.Append(partition = 1, events = 2, records = 2),
          Metrics.Append(partition = 0, events = 3, records = 2),
          Metrics.Append(partition = 0, events = 2, records = 2)))
    }

    "replicate expireAfter" in {
      val partition = 0
      val key = Key(id = "id", topic = topic)
      val topicPartition = topicPartitionOf(partition)
      val consumerRecords = ConsumerRecordsOf(List(
        consumerRecordOf(appendOf(key, Nel.of(1), 1.minute.toExpireAfter.some), topicPartition, 0),
        consumerRecordOf(appendOf(key, Nel.of(2), 2.minutes.toExpireAfter.some), topicPartition, 1)))

      val state = State(
        records = List(consumerRecords))
      val (result, _) = topicReplicator.run(state)

      result shouldEqual State(
        topics = List(topic),
        commits = List(Nem.of(
          (0, 2))),
        pointers = Map((topic, Map((0, 1L)))),
        journal = Map(
          ("id", List(
            record(seqNr = 1, partition = 0, offset = 0),
            record(seqNr = 2, partition = 0, offset = 1)))),
        metaJournal = Map(
          metaJournalOf("id", partition = 0, offset = 1, expireAfter = 2.minutes.toExpireAfter.some)),
        metrics = List(
          Metrics.Round(records = 2),
          Metrics.Append(partition = 0, events = 2, records = 2)))
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
            val offset = idx + 1L
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
          Nem.of((1, 5)),
          Nem.of((1, 4)),
          Nem.of((1, 3)),
          Nem.of((1, 2)),
          Nem.of((0, 5)),
          Nem.of((0, 4)),
          Nem.of((0, 3)),
          Nem.of((0, 2))),
        pointers = Map((topic, Map((0, 4L), (1, 4L)))),
        journal = Map(
          ("0-0", List(
            record(seqNr = 2, partition = 0, offset = 3),
            record(seqNr = 1, partition = 0, offset = 1))),
          ("0-1", List(
            record(seqNr = 3, partition = 0, offset = 4),
            record(seqNr = 1, partition = 0, offset = 2),
            record(seqNr = 2, partition = 0, offset = 2))),
          ("1-0", List(
            record(seqNr = 2, partition = 1, offset = 3),
            record(seqNr = 1, partition = 1, offset = 1))),
          ("1-1", List(
            record(seqNr = 3, partition = 1, offset = 4),
            record(seqNr = 1, partition = 1, offset = 2),
            record(seqNr = 2, partition = 1, offset = 2)))),
        metaJournal = Map(
          metaJournalOf("0-0", partition = 0, offset = 3),
          metaJournalOf("0-1", partition = 0, offset = 4),
          metaJournalOf("1-0", partition = 1, offset = 3),
          metaJournalOf("1-1", partition = 1, offset = 4)),
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
            val offset = idx + 1L
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
        commits = List(Nem.of(
          (0, 10),
          (1, 10),
          (2, 10))),
        pointers = Map((topic, Map((0, 9L), (1, 9L), (2, 9L)))),
        journal = Map(
          ("0-0", List(
            record(seqNr = 1, partition = 0, offset = 1),
            record(seqNr = 2, partition = 0, offset = 5))),
          ("0-1", List(
            record(seqNr = 1, partition = 0, offset = 3),
            record(seqNr = 2, partition = 0, offset = 3),
            record(seqNr = 3, partition = 0, offset = 8))),
          ("0-2", List(
            record(seqNr = 1, partition = 0, offset = 7),
            record(seqNr = 2, partition = 0, offset = 7),
            record(seqNr = 3, partition = 0, offset = 7))),
          ("1-0", List(
            record(seqNr = 1, partition = 1, offset = 1),
            record(seqNr = 2, partition = 1, offset = 5))),
          ("1-1", List(
            record(seqNr = 1, partition = 1, offset = 3),
            record(seqNr = 2, partition = 1, offset = 3),
            record(seqNr = 3, partition = 1, offset = 8))),
          ("1-2", List(
            record(seqNr = 1, partition = 1, offset = 7),
            record(seqNr = 2, partition = 1, offset = 7),
            record(seqNr = 3, partition = 1, offset = 7))),
          ("2-0", List(
            record(seqNr = 1, partition = 2, offset = 1),
            record(seqNr = 2, partition = 2, offset = 5))),
          ("2-1", List(
            record(seqNr = 1, partition = 2, offset = 3),
            record(seqNr = 2, partition = 2, offset = 3),
            record(seqNr = 3, partition = 2, offset = 8))),
          ("2-2", List(
            record(seqNr = 1, partition = 2, offset = 7),
            record(seqNr = 2, partition = 2, offset = 7),
            record(seqNr = 3, partition = 2, offset = 7)))),
        metaJournal = Map(
          metaJournalOf("0-0", partition = 0, offset = 9),
          metaJournalOf("0-1", partition = 0, offset = 8),
          metaJournalOf("0-2", partition = 0, offset = 7),
          metaJournalOf("1-0", partition = 1, offset = 9),
          metaJournalOf("1-1", partition = 1, offset = 8),
          metaJournalOf("1-2", partition = 1, offset = 7),
          metaJournalOf("2-0", partition = 2, offset = 9),
          metaJournalOf("2-1", partition = 2, offset = 8),
          metaJournalOf("2-2", partition = 2, offset = 7)),
        metrics = List(
          Metrics.Round(records = 27),
          Metrics.Append(partition = 2, events = 3, records = 1),
          Metrics.Append(partition = 2, events = 3, records = 2),
          Metrics.Append(partition = 2, events = 2, records = 2),
          Metrics.Append(partition = 1, events = 3, records = 1),
          Metrics.Append(partition = 1, events = 3, records = 2),
          Metrics.Append(partition = 1, events = 2, records = 2),
          Metrics.Append(partition = 0, events = 3, records = 1),
          Metrics.Append(partition = 0, events = 3, records = 2),
          Metrics.Append(partition = 0, events = 2, records = 2)))
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
            val offset = idx + 1L
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
        commits = List(Nem.of(
          (0, 11),
          (1, 11))),
        pointers = Map((topic, Map((0, 10L), (1, 10L)))),
        journal = Map(
          ("0-0", List(
            record(seqNr = 1, partition = 0, offset = 1),
            record(seqNr = 2, partition = 0, offset = 5))),
          ("0-1", List(
            record(seqNr = 3, partition = 0, offset = 8))),
          ("0-2", List(
            record(seqNr = 1, partition = 0, offset = 7),
            record(seqNr = 2, partition = 0, offset = 7),
            record(seqNr = 3, partition = 0, offset = 7))),
          ("1-0", List(
            record(seqNr = 1, partition = 1, offset = 1),
            record(seqNr = 2, partition = 1, offset = 5))),
          ("1-1", List(
            record(seqNr = 3, partition = 1, offset = 8))),
          ("1-2", List(
            record(seqNr = 1, partition = 1, offset = 7),
            record(seqNr = 2, partition = 1, offset = 7),
            record(seqNr = 3, partition = 1, offset = 7)))),
        metaJournal = Map(
          metaJournalOf("0-0", partition = 0, offset = 10),
          metaJournalOf("0-1", partition = 0, offset = 9, deleteTo = Some(2)),
          metaJournalOf("0-2", partition = 0, offset = 7),
          metaJournalOf("1-0", partition = 1, offset = 10),
          metaJournalOf("1-1", partition = 1, offset = 9, deleteTo = Some(2)),
          metaJournalOf("1-2", partition = 1, offset = 7)),
        metrics = List(
          Metrics.Round(records = 20),
          Metrics.Append(partition = 1, events = 3, records = 1),
          Metrics.Delete(partition = 1, actions = 1),
          Metrics.Append(partition = 1, events = 3, records = 2),
          Metrics.Append(partition = 1, events = 2, records = 2),
          Metrics.Append(partition = 0, events = 3, records = 1),
          Metrics.Delete(partition = 0, actions = 1),
          Metrics.Append(partition = 0, events = 3, records = 2),
          Metrics.Append(partition = 0, events = 2, records = 2)))
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
            val offset = idx + 1L
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
          Nem.of((0, 13)),
          Nem.of((0, 12)),
          Nem.of((0, 11)),
          Nem.of((0, 10)),
          Nem.of((0, 9)),
          Nem.of((0, 8)),
          Nem.of((0, 7)),
          Nem.of((0, 6)),
          Nem.of((0, 5)),
          Nem.of((0, 4)),
          Nem.of((0, 3)),
          Nem.of((0, 2))),
        pointers = Map(
          (topic, Map((0, 12L)))),
        journal = Map(
          ("0-0", Nil),
          ("0-1", List(
            record(seqNr = 3, partition = 0, offset = 8),
            record(seqNr = 1, partition = 0, offset = 3),
            record(seqNr = 2, partition = 0, offset = 3))),
          ("0-2", List(
            record(seqNr = 1, partition = 0, offset = 7),
            record(seqNr = 2, partition = 0, offset = 7),
            record(seqNr = 3, partition = 0, offset = 7)))),
        metaJournal = Map(
          metaJournalOf("0-0", partition = 0, offset = 11, deleteTo = Some(1)),
          metaJournalOf("0-1", partition = 0, offset = 12, deleteTo = Some(2)),
          metaJournalOf("0-2", partition = 0, offset = 7)),
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
      val pointers = Map((topic, Map((0, 1L), (2, 2L))))
      val data = State(pointers = pointers)
      val (result, _) = topicReplicator.run(data)
      result shouldEqual State(
        topics = List(topic),
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
        pointers = Map((topic, Map((0, 0L), (1, 1L), (2, 2L)))))
      val (result, _) = topicReplicator.run(data)

      result shouldEqual State(
        topics = List(topic),
        commits = List(Nem.of(
          (0, 4),
          (1, 4),
          (2, 4))),
        pointers = Map((topic, Map((0, 3L), (1, 3L), (2, 3L)))),
        journal = Map(
          ("0-0", List(
            record(seqNr = 2, partition = 0, offset = 2))),
          ("0-1", List(
            record(seqNr = 1, partition = 0, offset = 1),
            record(seqNr = 2, partition = 0, offset = 1),
            record(seqNr = 3, partition = 0, offset = 3))),
          ("1-0", List(
            record(seqNr = 2, partition = 1, offset = 2))),
          ("1-1", List(
            record(seqNr = 3, partition = 1, offset = 3))),
          ("2-1", List(
            record(seqNr = 3, partition = 2, offset = 3)))),
        metaJournal = Map(
          metaJournalOf("0-0", partition = 0, offset = 2),
          metaJournalOf("0-1", partition = 0, offset = 3),
          metaJournalOf("1-0", partition = 1, offset = 2),
          metaJournalOf("1-1", partition = 1, offset = 3),
          metaJournalOf("2-1", partition = 2, offset = 3)),
        metrics = List(
          Metrics.Round(records = 12),
          Metrics.Append(partition = 2, events = 1, records = 1),
          Metrics.Append(partition = 1, events = 1, records = 1),
          Metrics.Append(partition = 1, events = 1, records = 1),
          Metrics.Append(partition = 0, events = 3, records = 2),
          Metrics.Append(partition = 0, events = 1, records = 1)))
    }


    "ignore already replicated data and commit" in {
      val partition = 0
      val key = Key(id = "key", topic = topic)
      val action = appendOf(key, Nel.of(1))

      val topicPartition = topicPartitionOf(partition)
      val consumerRecord = consumerRecordOf(action, topicPartition, 0)
      val consumerRecords = ConsumerRecords(Map((consumerRecord.topicPartition, Nel.of(consumerRecord))))

      val data = State(
        records = List(consumerRecords),
        pointers = Map((topic, Map((0, 0L)))))
      val (result, _) = topicReplicator.run(data)

      result shouldEqual State(
        topics = List(topic),
        commits = List(Nem.of(
          (0, 1))),
        pointers = Map((topic, Map((0, 0L)))),
        metrics = List(Metrics.Round(records = 1)))
    }

    "replicate purge" in {
      val partition = 0
      val key = Key(id = "id", topic = topic)
      val topicPartition = topicPartitionOf(partition)
      val consumerRecords = ConsumerRecordsOf(List(
        consumerRecordOf(appendOf(key, Nel.of(1)), topicPartition, 0),
        consumerRecordOf(purgeOf(key), topicPartition, 1)))

      val state = State(
        records = List(consumerRecords))
      val (result, _) = topicReplicator.run(state)

      result shouldEqual State(
        topics = List(topic),
        commits = List(Nem.of(
          (0, 2))),
        pointers = Map((topic, Map((0, 1L)))),
        metrics = List(
          Metrics.Round(records = 2),
          Metrics.Purge(partition = 0, actions = 1)))
    }
  }

  private def consumerRecordOf(
    action: Action,
    topicPartition: TopicPartition,
    offset: Long
  ) = {

    val producerRecord = ActionToProducerRecord[Try].apply(action).get
    ConsumerRecord(
      topicPartition = topicPartition,
      offset = Offset.unsafe(offset),
      timestampAndType = Some(timestampAndType),
      key = producerRecord.key.map(WithSize(_)),
      value = producerRecord.value.map(WithSize(_)),
      headers = producerRecord.headers)
  }

  private def record(seqNr: Int, partition: Int, offset: Long) = {
    val partitionOffset = PartitionOffset(Partition.unsafe(partition), Offset.unsafe(offset))
    val event = Event(SeqNr.unsafe(seqNr), Set(seqNr.toString))
    EventRecord(event, timestamp, partitionOffset, Some(origin), recordMetadata, headers)
  }

  private def appendOf(key: Key, seqNrs: Nel[Int], expireAfter: Option[ExpireAfter] = none) = {
    implicit val eventsToPayload = EventsToPayload[Try]
    Action.Append.of[Try](
      key = key,
      timestamp = timestamp,
      origin = Some(origin),
      events = Events(
        events = seqNrs.map { seqNr => Event(SeqNr.unsafe(seqNr), Set(seqNr.toString)) }),
      metadata = recordMetadata,
      headers = headers,
      expireAfter = expireAfter
    ).get
  }

  private def markOf(key: Key) = {
    Action.Mark(key, timestamp, "id", Some(origin))
  }

  private def deleteOf(key: Key, to: Int) = {
    Action.Delete(key, timestamp, SeqNr.unsafe(to), Some(origin))
  }

  private def purgeOf(key: Key) = {
    Action.Purge(key, timestamp, Some(origin))
  }
}

object TopicReplicatorSpec {

  val topic = "topic"

  val timestamp: Instant = Instant.now()

  val origin = Origin("origin")

  val recordMetadata = RecordMetadata(Json.obj(("key", "value")).some)

  val headers = Headers(("key", "value"))

  val replicationLatency: FiniteDuration = 10.millis

  val timestampAndType = TimestampAndType(timestamp, TimestampType.Create)

  def topicPartitionOf(partition: Int): TopicPartition = TopicPartition(topic, Partition.unsafe(partition))

  def keyOf(id: String) = Key(id = id, topic = topic)

  def metaJournalOf(
    id: String,
    partition: Int,
    offset: Long,
    deleteTo: Option[Int] = none,
    expireAfter: Option[ExpireAfter] = none,
  ): (String, MetaJournal) = {
    val deleteToSeqNr = deleteTo.flatMap(deleteTo => SeqNr.opt(deleteTo.toLong))
    val partitionOffset = PartitionOffset(Partition.unsafe(partition), Offset.unsafe(offset))
    val metaJournal = MetaJournal(partitionOffset, deleteToSeqNr, expireAfter, origin.some)
    (id, metaJournal)
  }


  sealed abstract class Metrics extends Product

  object Metrics {

    final case class Append(
      partition: Int,
      latency: FiniteDuration = replicationLatency,
      events: Int,
      records: Int) extends Metrics

    final case class Delete(
      partition: Int,
      latency: FiniteDuration = replicationLatency,
      actions: Int) extends Metrics

    final case class Purge(
      partition: Int,
      latency: FiniteDuration = replicationLatency,
      actions: Int) extends Metrics

    final case class Round(
      duration: FiniteDuration = 0.millis,
      records: Int) extends Metrics
  }


  implicit def monoidDataF[A : Monoid]: Monoid[StateT[A]] = Applicative.monoid[StateT, A]


  implicit val replicatedJournal: ReplicatedJournal[StateT] = new ReplicatedJournal[StateT] {

    def journal(topic: Topic) = {

      val journal: ReplicatedTopicJournal[StateT] = new ReplicatedTopicJournal[StateT] {

        def pointers = {
          StateT { state =>
            val pointers = state
              .pointers
              .getOrElse(topic, Map.empty)
              .map { case (partition, offset) => (Partition.unsafe(partition), Offset.unsafe(offset)) }
            (state, TopicPointers(pointers))
          }
        }

        def journal(id: String) = {
          val journal = new ReplicatedKeyJournal[StateT] {

            def append(
              partitionOffset: PartitionOffset,
              timestamp: Instant,
              expireAfter: Option[ExpireAfter],
              events: Nel[EventRecord],
            ) = {
              StateT { state =>
                val records = events.toList ++ state.journal.getOrElse(id, Nil)

                val deleteTo = state.metaJournal.get(id).flatMap(_.deleteTo)

                val metaJournal = MetaJournal(partitionOffset, deleteTo, expireAfter, events.last.origin)

                val state1 = state.copy(
                  journal = state.journal.updated(id, records),
                  metaJournal = state.metaJournal.updated(id, metaJournal))

                (state1, ())
              }
            }

            def delete(partitionOffset: PartitionOffset, timestamp: Instant, deleteTo: SeqNr, origin: Option[Origin]) = {
              StateT { state => (state.delete(id, deleteTo, partitionOffset, origin), ()) }
            }

            def purge(
              offset: Offset,
              timestamp: Instant,
            ) = {
              StateT { state =>
                val state1 = state.copy(
                  journal = state.journal - id,
                  metaJournal = state.metaJournal - id)
                (state1, ())
              }
            }
          }

          Resource.liftF(journal.pure[StateT])
        }

        def save(pointers: Nem[Partition, Offset], timestamp: Instant) = {
          StateT { state =>
            val pointers1 = pointers
              .toSortedMap
              .map { case (partition, offset) => (partition.value, offset.value)}
            val pointers2 = state
              .pointers
              .getOrElse(topic, Map.empty) ++ pointers1
            val state1 = state.copy(pointers = state.pointers.updated(topic, pointers2))
            (state1, ())
          }
        }
      }

      Resource.liftF(journal.pure[StateT])
    }

    def topics = List.empty[Topic].pure[StateT]
  }


  implicit val consumer: TopicConsumer[StateT] = new TopicConsumer[StateT] {

    def subscribe(listener: RebalanceListener[StateT]) = {
      StateT { s => (s.subscribe(topic), ()) }
    }

    val commit = offsets => {
      StateT.unit { state =>
        val offsets1 = offsets.mapKV { case (partition, offset) => ((partition.value, offset.value)) }
        state.copy(commits = offsets1 :: state.commits)
      }
    }

    def poll = {
      val records = StateT { state =>
        state.records match {
          case head :: tail =>
            val records = for {
              (partition, records) <- head.values
            } yield {
              (partition.partition, records)
            }
            (state.copy(records = tail), records.some)
            
          case Nil          => (state, none)
        }
      }
      Stream.whileSome(records)
    }
  }


  implicit val parallelStateT: Parallel[StateT] = Parallel.identity[StateT]


  implicit val metrics: TopicReplicator.Metrics[StateT] = new TopicReplicator.Metrics[StateT] {

    def append(events: Int, bytes: Long, measurements: Measurements) = {
      StateT {
        _ + Metrics.Append(
          partition = measurements.partition.value,
          latency = measurements.replicationLatency,
          events = events,
          records = measurements.records)
      }
    }

    def delete(measurements: Measurements) = {
      StateT {
        _ + Metrics.Delete(
          partition = measurements.partition.value,
          latency = measurements.replicationLatency,
          actions = measurements.records)
      }
    }

    def purge(measurements: Measurements) = {
      StateT {
        _ + Metrics.Purge(
          partition = measurements.partition.value,
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
    implicit val fail = Fail.lift[StateT]
    implicit val fromTry = FromTry.lift[StateT]
    implicit val fromAttempt = FromAttempt.lift[StateT]
    implicit val fromJsResult = FromJsResult.lift[StateT]

    implicit val timer: Timer[StateT] = new Timer[StateT] {

      val clock = Clock.const[StateT](nanos = 0, millis = millis)

      def sleep(duration: FiniteDuration) = ().pure[StateT]
    }

    implicit val measureDuration = MeasureDuration.fromClock(timer.clock)

    TopicReplicator.of[StateT](
      topic = topic,
      consumer = Resource.liftF(consumer.pure[StateT]),
      consRecordToActionRecord = ConsRecordToActionRecord[StateT],
      payloadToEvents = PayloadToEvents[StateT],
      journal = replicatedJournal,
      metrics = metrics,
      log = Log.empty[StateT],
      cacheOf = CacheOf.empty[StateT])
  }


  final case class State(
    topics: List[Topic] = Nil,
    commits: List[Nem[Int, Long]] = Nil,
    records: List[ConsRecords] = Nil,
    pointers: Map[Topic, Map[Int, Long]] = Map.empty,
    journal: Map[String, List[EventRecord]] = Map.empty,
    metaJournal: Map[String, MetaJournal] = Map.empty,
    metrics: List[Metrics] = Nil
  ) { self =>

    def +(metrics: Metrics): (State, Unit) = {
      val result = copy(metrics = metrics :: self.metrics)
      (result, ())
    }

    def subscribe(topic: Topic): State = {
      copy(topics = topic :: topics)
    }

    def delete(id: String, deleteTo: SeqNr, partitionOffset: PartitionOffset, origin: Option[Origin]): State = {

      def journal = self.journal.getOrElse(id, Nil)

      def delete(deleteTo: SeqNr) = journal.dropWhile(_.seqNr <= deleteTo)

      val deleteTo1 = self.metaJournal.get(id).flatMap(_.deleteTo)
      if (deleteTo1.exists(_ >= deleteTo)) {
        self.metaJournal.get(id).fold(this) { metaJournal =>
          copy(metaJournal = self.metaJournal.updated(id, metaJournal.copy(offset = partitionOffset)))
        }
      } else {
        val records = delete(deleteTo)
        val result = records.headOption.flatMap(_.seqNr.prev[Option]) orElse journal.lastOption.map(_.seqNr)
        val metaJournal = MetaJournal(
          offset = partitionOffset,
          deleteTo = result orElse deleteTo1,
          expireAfter = none,
          origin = origin)
        copy(
          journal = self.journal.updated(id, records),
          metaJournal = self.metaJournal.updated(id, metaJournal))
      }
    }
  }


  type StateT[A] = cats.data.StateT[Id, State, A]

  object StateT {

    def apply[A](f: State => (State, A)): StateT[A] = cats.data.StateT[Id, State, A](f)

    def unit(f: State => State): StateT[Unit] = apply { state =>
      val state1 = f(state)
      (state1, ())
    }
  }


  final case class MetaJournal(
    offset: PartitionOffset,
    deleteTo: Option[SeqNr],
    expireAfter: Option[ExpireAfter],
    origin: Option[Origin])

  case object NotImplemented extends RuntimeException with NoStackTrace
}
