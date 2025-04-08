package com.evolutiongaming.kafka.journal.replicator

import cats.data.{NonEmptyList as Nel, NonEmptyMap as Nem}
import cats.effect.*
import cats.effect.syntax.resource.*
import cats.effect.unsafe.implicits.global
import cats.syntax.all.*
import cats.{Applicative, Monoid, Parallel}
import com.evolutiongaming.catshelper.ClockHelper.*
import com.evolutiongaming.catshelper.DataHelper.*
import com.evolutiongaming.catshelper.{FromTry, Log, MeasureDuration}
import com.evolutiongaming.kafka.journal.*
import com.evolutiongaming.kafka.journal.ExpireAfter.implicits.*
import com.evolutiongaming.kafka.journal.TestJsonCodec.instance
import com.evolutiongaming.kafka.journal.conversions.{
  ActionToProducerRecord,
  ConsRecordToActionRecord,
  KafkaRead,
  KafkaWrite,
}
import com.evolutiongaming.kafka.journal.eventual.*
import com.evolutiongaming.kafka.journal.replicator.TopicReplicatorMetrics.Measurements
import com.evolutiongaming.kafka.journal.util.{Fail, TestTemporal}
import com.evolutiongaming.retry.Sleep
import com.evolutiongaming.skafka.consumer.{ConsumerRecord, ConsumerRecords, RebalanceListener1, WithSize}
import com.evolutiongaming.skafka.{Bytes as _, Header as _, Metadata as _, *}
import com.evolutiongaming.sstream.Stream
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec
import play.api.libs.json.Json

import java.time.Instant
import scala.collection.immutable.SortedSet
import scala.concurrent.duration.*
import scala.util.Try
import scala.util.control.NoStackTrace

class TopicReplicatorSpec extends AsyncWordSpec with Matchers {
  import TopicReplicatorSpec.*

  "TopicReplicator" should {

    "replicate appends" in {

      val records = {
        val records = for {
          partition <- 0 to 1
          keyOf = (id: String) => Key(id = s"$partition-$id", topic = topic)
          append = (id: String, seqNrs: Nel[Int]) => appendOf(keyOf(id), seqNrs)
          topicPartition = topicPartitionOf(partition)
          actions =
            List(append("0", Nel.of(1)), append("1", Nel.of(1, 2)), append("0", Nel.of(2)), append("1", Nel.of(3)))
          (action, idx) <- actions.zipWithIndex
          offset = idx + 1L
        } yield {
          consumerRecordOf(action, topicPartition, offset)
        }
        ConsumerRecordsOf(records.toList)
      }

      val data = State(records = List(records))

      topicReplicator.run(data).unsafeToFuture().map {
        case (result, _) =>
          result shouldEqual State(
            topics = List(topic),
            commits = List(Nem.of((0, 5), (1, 5))),
            pointers = Map((topic, Map((0, 4L), (1, 4L)))),
            replicatedOffsetNotifications = Map((topic, Map((0, Vector(4L)), (1, Vector(4L))))),
            journal = Map(
              ("0-0", List(record(seqNr = 1, partition = 0, offset = 1), record(seqNr = 2, partition = 0, offset = 3))),
              (
                "0-1",
                List(
                  record(seqNr = 1, partition = 0, offset = 2),
                  record(seqNr = 2, partition = 0, offset = 2),
                  record(seqNr = 3, partition = 0, offset = 4),
                ),
              ),
              ("1-0", List(record(seqNr = 1, partition = 1, offset = 1), record(seqNr = 2, partition = 1, offset = 3))),
              (
                "1-1",
                List(
                  record(seqNr = 1, partition = 1, offset = 2),
                  record(seqNr = 2, partition = 1, offset = 2),
                  record(seqNr = 3, partition = 1, offset = 4),
                ),
              ),
            ),
            metaJournal = Map(
              metaJournalOf("0-0", partition = 0, offset = 3),
              metaJournalOf("0-1", partition = 0, offset = 4),
              metaJournalOf("1-0", partition = 1, offset = 3),
              metaJournalOf("1-1", partition = 1, offset = 4),
            ),
            metrics = List(
              Metrics.Round(records = 8),
              Metrics.Append(events = 3, records = 2),
              Metrics.Append(events = 2, records = 2),
              Metrics.Append(events = 3, records = 2),
              Metrics.Append(events = 2, records = 2),
            ),
          )
      }
    }

    "replicate expireAfter" in {
      val partition = 0
      val key = Key(id = "id", topic = topic)
      val topicPartition = topicPartitionOf(partition)
      val consumerRecords = ConsumerRecordsOf(
        List(
          consumerRecordOf(appendOf(key, Nel.of(1), 1.minute.toExpireAfter.some), topicPartition, 0),
          consumerRecordOf(appendOf(key, Nel.of(2), 2.minutes.toExpireAfter.some), topicPartition, 1),
        ),
      )

      val state = State(records = List(consumerRecords))

      topicReplicator.run(state).unsafeToFuture().map {
        case (result, _) =>
          result shouldEqual State(
            topics = List(topic),
            commits = List(Nem.of((0, 2))),
            pointers = Map((topic, Map((0, 1L)))),
            replicatedOffsetNotifications = Map((topic, Map((0, Vector(1L))))),
            journal = Map(
              (
                "id",
                List(
                  record(seqNr = 1, partition = 0, offset = 0, 1.minute.toExpireAfter.some),
                  record(seqNr = 2, partition = 0, offset = 1, 2.minutes.toExpireAfter.some),
                ),
              ),
            ),
            metaJournal =
              Map(metaJournalOf("id", partition = 0, offset = 1, expireAfter = 2.minutes.toExpireAfter.some)),
            metrics = List(Metrics.Round(records = 2), Metrics.Append(events = 2, records = 2)),
          )
      }
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

          val kafkaRecords =
            List(append("0", Nel.of(1)), append("1", Nel.of(1, 2)), append("0", Nel.of(2)), append("1", Nel.of(3)))

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

      topicReplicator.run(data).unsafeToFuture().map {
        case (result, _) =>
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
              Nem.of((0, 2)),
            ),
            pointers = Map((topic, Map((0, 4L), (1, 4L)))),
            replicatedOffsetNotifications = Map((topic, Map((0, Vector(1L, 2L, 3L, 4L)), (1, Vector(1L, 2L, 3L, 4L))))),
            journal = Map(
              ("0-0", List(record(seqNr = 2, partition = 0, offset = 3), record(seqNr = 1, partition = 0, offset = 1))),
              (
                "0-1",
                List(
                  record(seqNr = 3, partition = 0, offset = 4),
                  record(seqNr = 1, partition = 0, offset = 2),
                  record(seqNr = 2, partition = 0, offset = 2),
                ),
              ),
              ("1-0", List(record(seqNr = 2, partition = 1, offset = 3), record(seqNr = 1, partition = 1, offset = 1))),
              (
                "1-1",
                List(
                  record(seqNr = 3, partition = 1, offset = 4),
                  record(seqNr = 1, partition = 1, offset = 2),
                  record(seqNr = 2, partition = 1, offset = 2),
                ),
              ),
            ),
            metaJournal = Map(
              metaJournalOf("0-0", partition = 0, offset = 3),
              metaJournalOf("0-1", partition = 0, offset = 4),
              metaJournalOf("1-0", partition = 1, offset = 3),
              metaJournalOf("1-1", partition = 1, offset = 4),
            ),
            metrics = List(
              Metrics.Round(records = 1),
              Metrics.Append(events = 1, records = 1),
              Metrics.Round(records = 1),
              Metrics.Append(events = 1, records = 1),
              Metrics.Round(records = 1),
              Metrics.Append(events = 2, records = 1),
              Metrics.Round(records = 1),
              Metrics.Append(events = 1, records = 1),
              Metrics.Round(records = 1),
              Metrics.Append(events = 1, records = 1),
              Metrics.Round(records = 1),
              Metrics.Append(events = 1, records = 1),
              Metrics.Round(records = 1),
              Metrics.Append(events = 2, records = 1),
              Metrics.Round(records = 1),
              Metrics.Append(events = 1, records = 1),
            ),
          )
      }

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
            mark("0"),
          )

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
      topicReplicator.run(data).unsafeToFuture().map {
        case (result, _) =>
          result shouldEqual State(
            topics = List(topic),
            commits = List(Nem.of((0, 10), (1, 10), (2, 10))),
            pointers = Map((topic, Map((0, 9L), (1, 9L), (2, 9L)))),
            replicatedOffsetNotifications = Map((topic, Map((0, Vector(9L)), (1, Vector(9L)), (2, Vector(9L))))),
            journal = Map(
              ("0-0", List(record(seqNr = 1, partition = 0, offset = 1), record(seqNr = 2, partition = 0, offset = 5))),
              (
                "0-1",
                List(
                  record(seqNr = 1, partition = 0, offset = 3),
                  record(seqNr = 2, partition = 0, offset = 3),
                  record(seqNr = 3, partition = 0, offset = 8),
                ),
              ),
              (
                "0-2",
                List(
                  record(seqNr = 1, partition = 0, offset = 7),
                  record(seqNr = 2, partition = 0, offset = 7),
                  record(seqNr = 3, partition = 0, offset = 7),
                ),
              ),
              ("1-0", List(record(seqNr = 1, partition = 1, offset = 1), record(seqNr = 2, partition = 1, offset = 5))),
              (
                "1-1",
                List(
                  record(seqNr = 1, partition = 1, offset = 3),
                  record(seqNr = 2, partition = 1, offset = 3),
                  record(seqNr = 3, partition = 1, offset = 8),
                ),
              ),
              (
                "1-2",
                List(
                  record(seqNr = 1, partition = 1, offset = 7),
                  record(seqNr = 2, partition = 1, offset = 7),
                  record(seqNr = 3, partition = 1, offset = 7),
                ),
              ),
              ("2-0", List(record(seqNr = 1, partition = 2, offset = 1), record(seqNr = 2, partition = 2, offset = 5))),
              (
                "2-1",
                List(
                  record(seqNr = 1, partition = 2, offset = 3),
                  record(seqNr = 2, partition = 2, offset = 3),
                  record(seqNr = 3, partition = 2, offset = 8),
                ),
              ),
              (
                "2-2",
                List(
                  record(seqNr = 1, partition = 2, offset = 7),
                  record(seqNr = 2, partition = 2, offset = 7),
                  record(seqNr = 3, partition = 2, offset = 7),
                ),
              ),
            ),
            metaJournal = Map(
              metaJournalOf("0-0", partition = 0, offset = 5),
              metaJournalOf("0-1", partition = 0, offset = 8),
              metaJournalOf("0-2", partition = 0, offset = 7),
              metaJournalOf("1-0", partition = 1, offset = 5),
              metaJournalOf("1-1", partition = 1, offset = 8),
              metaJournalOf("1-2", partition = 1, offset = 7),
              metaJournalOf("2-0", partition = 2, offset = 5),
              metaJournalOf("2-1", partition = 2, offset = 8),
              metaJournalOf("2-2", partition = 2, offset = 7),
            ),
            metrics = List(
              Metrics.Round(records = 27),
              Metrics.Append(events = 3, records = 1),
              Metrics.Append(events = 3, records = 2),
              Metrics.Append(events = 2, records = 2),
              Metrics.Append(events = 3, records = 1),
              Metrics.Append(events = 3, records = 2),
              Metrics.Append(events = 2, records = 2),
              Metrics.Append(events = 3, records = 1),
              Metrics.Append(events = 3, records = 2),
              Metrics.Append(events = 2, records = 2),
            ),
          )
      }
    }

    "replicate when mark only" in {
      val tp0 = topicPartitionOf(0)
      val tp1 = topicPartitionOf(1)

      def keyOf(tp: TopicPartition, id: String) = Key(id = s"${ tp.partition }-$id", topic = tp.topic)

      val records = ConsumerRecords(
        Map(
          tp0 -> Nel.of(
            consumerRecordOf(markOf(keyOf(tp0, "1")), tp0, offset = 1L),
          ),
          tp1 -> Nel.of(
            consumerRecordOf(markOf(keyOf(tp1, "1")), tp1, offset = 10L),
            consumerRecordOf(markOf(keyOf(tp1, "2")), tp1, offset = 11L),
          ),
        ),
      )

      val data = State(records = List(records))
      topicReplicator.run(data).unsafeToFuture().map {
        case (result, _) =>
          result shouldEqual State(
            topics = List(topic),
            commits = List(Nem.of(0 -> 2L, 1 -> 12L)),
            pointers = Map(topic -> Map(0 -> 1L, 1 -> 11L)),
            replicatedOffsetNotifications = Map(topic -> Map(0 -> Vector(1L), 1 -> Vector(11L))),
            journal = Map.empty,
            metaJournal = Map.empty,
            metrics = List(Metrics.Round(records = 3)),
          )
      }
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
            mark("0"),
          )

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
      topicReplicator.run(data).unsafeToFuture().map {
        case (result, _) =>
          result shouldEqual State(
            topics = List(topic),
            commits = List(Nem.of((0, 11), (1, 11))),
            pointers = Map((topic, Map((0, 10L), (1, 10L)))),
            replicatedOffsetNotifications = Map((topic, Map((0, Vector(10L)), (1, Vector(10L))))),
            journal = Map(
              ("0-0", List(record(seqNr = 1, partition = 0, offset = 1), record(seqNr = 2, partition = 0, offset = 5))),
              ("0-1", List(record(seqNr = 3, partition = 0, offset = 8))),
              (
                "0-2",
                List(
                  record(seqNr = 1, partition = 0, offset = 7),
                  record(seqNr = 2, partition = 0, offset = 7),
                  record(seqNr = 3, partition = 0, offset = 7),
                ),
              ),
              ("1-0", List(record(seqNr = 1, partition = 1, offset = 1), record(seqNr = 2, partition = 1, offset = 5))),
              ("1-1", List(record(seqNr = 3, partition = 1, offset = 8))),
              (
                "1-2",
                List(
                  record(seqNr = 1, partition = 1, offset = 7),
                  record(seqNr = 2, partition = 1, offset = 7),
                  record(seqNr = 3, partition = 1, offset = 7),
                ),
              ),
            ),
            metaJournal = Map(
              metaJournalOf("0-0", partition = 0, offset = 5),
              metaJournalOf("0-1", partition = 0, offset = 9, deleteTo = 2.some),
              metaJournalOf("0-2", partition = 0, offset = 7),
              metaJournalOf("1-0", partition = 1, offset = 5),
              metaJournalOf("1-1", partition = 1, offset = 9, deleteTo = 2.some),
              metaJournalOf("1-2", partition = 1, offset = 7),
            ),
            metrics = List(
              Metrics.Round(records = 20),
              Metrics.Append(events = 3, records = 1),
              Metrics.Delete(actions = 1),
              Metrics.Append(events = 1, records = 1),
              Metrics.Append(events = 2, records = 2),
              Metrics.Append(events = 3, records = 1),
              Metrics.Delete(actions = 1),
              Metrics.Append(events = 1, records = 1),
              Metrics.Append(events = 2, records = 2),
            ),
          )
      }
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
            delete("1", 2),
          )

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
      topicReplicator.run(data).unsafeToFuture().map {
        case (result, _) =>
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
              Nem.of((0, 2)),
            ),
            pointers = Map((topic, Map((0, 12L)))),
            replicatedOffsetNotifications = Map((topic, Map((0, Vector(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12))))),
            journal = Map(
              ("0-0", Nil),
              (
                "0-1",
                List(
                  record(seqNr = 3, partition = 0, offset = 8),
                  record(seqNr = 1, partition = 0, offset = 3),
                  record(seqNr = 2, partition = 0, offset = 3),
                ),
              ),
              (
                "0-2",
                List(
                  record(seqNr = 1, partition = 0, offset = 7),
                  record(seqNr = 2, partition = 0, offset = 7),
                  record(seqNr = 3, partition = 0, offset = 7),
                ),
              ),
            ),
            metaJournal = Map(
              metaJournalOf("0-0", partition = 0, offset = 11, deleteTo = 1.some),
              metaJournalOf("0-1", partition = 0, offset = 12, deleteTo = 2.some),
              metaJournalOf("0-2", partition = 0, offset = 7),
            ),
            metrics = List(
              Metrics.Round(records = 1),
              Metrics.Delete(actions = 1),
              Metrics.Round(records = 1),
              Metrics.Delete(actions = 1),
              Metrics.Round(records = 1),
              Metrics.Delete(actions = 1),
              Metrics.Round(records = 1),
              Metrics.Delete(actions = 1),
              Metrics.Round(records = 1),
              Metrics.Append(events = 1, records = 1),
              Metrics.Round(records = 1),
              Metrics.Append(events = 3, records = 1),
              Metrics.Round(records = 1),
              Metrics.Round(records = 1),
              Metrics.Append(events = 1, records = 1),
              Metrics.Round(records = 1),
              Metrics.Round(records = 1),
              Metrics.Append(events = 2, records = 1),
              Metrics.Round(records = 1),
              Metrics.Round(records = 1),
              Metrics.Append(events = 1, records = 1),
            ),
          )
      }
    }

    "consume since replicated offset" in {
      val pointers = Map((topic, Map((0, 1L), (2, 2L))))
      val data = State(pointers = pointers)
      topicReplicator.run(data).unsafeToFuture().map {
        case (result, _) =>
          result shouldEqual State(topics = List(topic), pointers = pointers)
      }
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

          val actions =
            List(append("0", Nel.of(1)), append("1", Nel.of(1, 2)), append("0", Nel.of(2)), append("1", Nel.of(3)))

          val records = for {
            (action, offset) <- actions.zipWithIndex
          } yield {
            consumerRecordOf(action, topicPartition, offset.toLong)
          }
          (topicPartition, Nel.fromListUnsafe(records))
        }
        ConsumerRecords(records.toMap)
      }

      val data = State(records = List(records), pointers = Map((topic, Map((0, 0L), (1, 1L), (2, 2L)))))
      topicReplicator.run(data).unsafeToFuture().map {
        case (result, _) =>
          result shouldEqual State(
            topics = List(topic),
            commits = List(Nem.of((0, 4), (1, 4), (2, 4))),
            pointers = Map((topic, Map((0, 3L), (1, 3L), (2, 3L)))),
            replicatedOffsetNotifications = Map((topic, Map((0, Vector(3L)), (1, Vector(3L)), (2, Vector(3L))))),
            journal = Map(
              ("0-0", List(record(seqNr = 2, partition = 0, offset = 2))),
              (
                "0-1",
                List(
                  record(seqNr = 1, partition = 0, offset = 1),
                  record(seqNr = 2, partition = 0, offset = 1),
                  record(seqNr = 3, partition = 0, offset = 3),
                ),
              ),
              ("1-0", List(record(seqNr = 2, partition = 1, offset = 2))),
              ("1-1", List(record(seqNr = 3, partition = 1, offset = 3))),
              ("2-1", List(record(seqNr = 3, partition = 2, offset = 3))),
            ),
            metaJournal = Map(
              metaJournalOf("0-0", partition = 0, offset = 2),
              metaJournalOf("0-1", partition = 0, offset = 3),
              metaJournalOf("1-0", partition = 1, offset = 2),
              metaJournalOf("1-1", partition = 1, offset = 3),
              metaJournalOf("2-1", partition = 2, offset = 3),
            ),
            metrics = List(
              Metrics.Round(records = 12),
              Metrics.Append(events = 1, records = 1),
              Metrics.Append(events = 1, records = 1),
              Metrics.Append(events = 1, records = 1),
              Metrics.Append(events = 3, records = 2),
              Metrics.Append(events = 1, records = 1),
            ),
          )
      }
    }

    "ignore already replicated data and commit" in {
      val partition = 0
      val key = Key(id = "key", topic = topic)
      val action = appendOf(key, Nel.of(1))

      val topicPartition = topicPartitionOf(partition)
      val consumerRecord = consumerRecordOf(action, topicPartition, 0)
      val consumerRecords = ConsumerRecords(Map((consumerRecord.topicPartition, Nel.of(consumerRecord))))

      val data = State(records = List(consumerRecords), pointers = Map((topic, Map((0, 0L)))))
      topicReplicator.run(data).unsafeToFuture().map {
        case (result, _) =>
          result shouldEqual State(
            topics = List(topic),
            commits = List(Nem.of((0, 1))),
            pointers = Map((topic, Map((0, 0L)))),
            metrics = List(Metrics.Round(records = 1)),
          )
      }
    }

    "replicate purge" in {
      val partition = 0
      val key = Key(id = "id", topic = topic)
      val topicPartition = topicPartitionOf(partition)
      val consumerRecords = ConsumerRecordsOf(
        List(
          consumerRecordOf(appendOf(key, Nel.of(1)), topicPartition, 0),
          consumerRecordOf(purgeOf(key), topicPartition, 1),
        ),
      )

      val state =
        State(records = List(consumerRecords), metaJournal = Map(metaJournalOf(key.id, partition = 0, offset = 0)))
      topicReplicator
        .run(state)
        .map {
          case (result, _) =>
            result shouldEqual State(
              topics = List(topic),
              commits = List(Nem.of((0, 2))),
              pointers = Map((topic, Map((0, 1L)))),
              replicatedOffsetNotifications = Map((topic, Map((0, Vector(1L))))),
              metrics = List(Metrics.Round(records = 2), Metrics.Purge(actions = 1)),
            )
        }
        .unsafeToFuture()
    }
  }

  private def consumerRecordOf(
    action: Action,
    topicPartition: TopicPartition,
    offset: Long,
  ) = {

    val producerRecord = ActionToProducerRecord[Try].apply(action).get
    ConsumerRecord(
      topicPartition = topicPartition,
      offset = Offset.unsafe(offset),
      timestampAndType = timestampAndType.some,
      key = producerRecord.key.map(WithSize(_)),
      value = producerRecord.value.map(WithSize(_)),
      headers = producerRecord.headers,
    )
  }

  private def record(
    seqNr: Int,
    partition: Int,
    offset: Long,
    expireAfter: Option[ExpireAfter] = none,
  ) = {
    val partitionOffset = PartitionOffset(Partition.unsafe(partition), Offset.unsafe(offset))
    val event = Event[EventualPayloadAndType](SeqNr.unsafe(seqNr), Set(seqNr.toString))
    EventRecord(
      event,
      timestamp,
      partitionOffset,
      origin.some,
      version.some,
      recordMetadata.withExpireAfter(expireAfter),
      headers,
    )
  }

  private def appendOf(key: Key, seqNrs: Nel[Int], expireAfter: Option[ExpireAfter] = none) = {
    implicit val kafkaWrite = KafkaWrite.summon[Try, Payload]
    Action
      .Append
      .of[Try, Payload](
        key = key,
        timestamp = timestamp,
        origin = origin.some,
        version = version.some,
        events = Events(
          events = seqNrs.map { seqNr => Event(SeqNr.unsafe(seqNr), Set(seqNr.toString)) },
          recordMetadata.payload.copy(expireAfter = expireAfter),
        ),
        metadata = recordMetadata.header,
        headers = headers,
      )
      .get
  }

  private def markOf(key: Key) = {
    Action.Mark(key, timestamp, "id", origin.some, version.some)
  }

  private def deleteOf(key: Key, to: Int) = {
    Action.Delete(key, timestamp, SeqNr.unsafe(to).toDeleteTo, origin.some, version.some)
  }

  private def purgeOf(key: Key) = {
    Action.Purge(key, timestamp, origin.some, version.some)
  }
}

object TopicReplicatorSpec {

  val topic = "topic"

  val timestamp: Instant = Instant.now()

  val origin: Origin = Origin("origin")

  val version: Version = Version.current

  val recordMetadata: RecordMetadata =
    RecordMetadata(HeaderMetadata(Json.obj(("key", "value")).some), PayloadMetadata.empty)

  val headers: Headers = Headers(("key", "value"))

  val replicationLatency: FiniteDuration = 10.millis

  val timestampAndType: TimestampAndType = TimestampAndType(timestamp, TimestampType.Create)

  def topicPartitionOf(partition: Int): TopicPartition = TopicPartition(topic, Partition.unsafe(partition))

  def keyOf(id: String) = Key(id = id, topic = topic)

  def metaJournalOf(
    id: String,
    partition: Int,
    offset: Long,
    deleteTo: Option[Int] = none,
    expireAfter: Option[ExpireAfter] = none,
  ): (String, MetaJournal) = {
    val deleteToSeqNr = deleteTo
      .flatMap { deleteTo => SeqNr.opt(deleteTo.toLong) }
      .map { _.toDeleteTo }
    val metaJournal =
      MetaJournal(
        PartitionOffset(Partition.unsafe(partition), Offset.unsafe(offset)),
        deleteToSeqNr,
        expireAfter,
        origin.some,
      )
    (id, metaJournal)
  }

  sealed abstract class Metrics extends Product

  object Metrics {

    final case class Append(latency: FiniteDuration = replicationLatency, events: Int, records: Int) extends Metrics

    final case class Delete(latency: FiniteDuration = replicationLatency, actions: Int) extends Metrics

    final case class Purge(latency: FiniteDuration = replicationLatency, actions: Int) extends Metrics

    final case class Round(duration: FiniteDuration = 0.millis, records: Int) extends Metrics
  }

  implicit def monoidDataF[A: Monoid]: Monoid[StateT[A]] = Applicative.monoid[StateT, A]

  implicit val replicatedJournal: ReplicatedJournal[StateT] = new ReplicatedJournal[StateT] {

    def journal(topic: Topic) = {

      val journal: ReplicatedTopicJournal[StateT] = new ReplicatedTopicJournal[StateT] {

        def apply(partition: Partition) = {

          val result = new ReplicatedPartitionJournal[StateT] {

            def offsets = {
              new ReplicatedPartitionJournal.Offsets[StateT] {

                def get = {
                  StateT { state =>
                    val offset = state
                      .pointers
                      .getOrElse(topic, Map.empty)
                      .get(partition.value)
                      .map(Offset.unsafe[Long])
                    (state, offset)
                  }
                }

                def create(offset: Offset, timestamp: Instant) = {
                  update(offset, timestamp)
                }

                def update(offset: Offset, timestamp: Instant) = {
                  StateT { state =>
                    val pointers1 = state
                      .pointers
                      .getOrElse(topic, Map.empty)
                      .updated(partition.value, offset.value)
                    val state1 = state.copy(pointers = state.pointers.updated(topic, pointers1))
                    (state1, ())
                  }
                }
              }
            }

            def journal(id: ClientId) = {
              val result = new ReplicatedKeyJournal[StateT] {

                def append(
                  offset: Offset,
                  timestamp: Instant,
                  expireAfter: Option[ExpireAfter],
                  events: Nel[EventRecord[EventualPayloadAndType]],
                ) = {
                  StateT { state =>
                    val records = events.toList ++ state.journal.getOrElse(id, Nil)

                    val deleteTo = state.metaJournal.get(id).flatMap(_.deleteTo)

                    val metaJournal =
                      MetaJournal(PartitionOffset(partition, offset), deleteTo, expireAfter, events.last.origin)

                    val state1 = state.copy(
                      journal = state.journal.updated(id, records),
                      metaJournal = state.metaJournal.updated(id, metaJournal),
                    )

                    val updated = state
                      .metaJournal
                      .get(id)
                      .fold(true) { journalHead =>
                        offset > journalHead.offset.offset
                      }
                    (state1, updated)
                  }
                }

                def delete(
                  offset: Offset,
                  timestamp: Instant,
                  deleteTo: DeleteTo,
                  origin: Option[Origin],
                ) = {
                  StateT { state =>
                    val deleted = state.metaJournal.get(id).fold(true) { journalHead =>
                      offset > journalHead.offset.offset
                    }

                    (state.delete(id, deleteTo, PartitionOffset(partition, offset), origin), deleted)
                  }
                }

                def purge(offset: Offset, timestamp: Instant) = {
                  StateT { state =>
                    val state1 = state.copy(journal = state.journal - id, metaJournal = state.metaJournal - id)

                    val purged = state.metaJournal.get(id).fold(false) { journalHead =>
                      offset > journalHead.offset.offset
                    }

                    (state1, purged)
                  }
                }
              }

              result.pure[StateT].toResource
            }
          }
          result.pure[Resource[StateT, *]]
        }
      }

      journal.pure[StateT].toResource
    }

    def topics = SortedSet.empty[Topic].pure[StateT]
  }

  val replicatedOffsetNotifier: ReplicatedOffsetNotifier[StateT] =
    (topicPartition: TopicPartition, offset: Offset) => {
      StateT.unit { state =>
        state.addReplicatedOffsetNotification(topicPartition, offset.value)
      }
    }

  implicit val consumer: TopicConsumer[StateT] = new TopicConsumer[StateT] {

    def subscribe(listener: RebalanceListener1[StateT]) = {
      StateT { s =>
        (s.subscribe(topic), ())
      }
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

          case Nil => (state, none)
        }
      }
      Stream.whileSome(records)
    }
  }

  implicit val parallelStateT: Parallel[StateT] = Parallel.identity[StateT]

  implicit val metrics: TopicReplicatorMetrics[StateT] = new TopicReplicatorMetrics[StateT] {

    def append(
      events: Int,
      bytes: Long,
      clientVersion: String,
      expiration: String,
      measurements: Measurements,
    ) = {
      StateT { s =>
        s + Metrics.Append(latency = measurements.replicationLatency, events = events, records = measurements.records)
      }
    }

    def delete(measurements: Measurements) = {
      StateT { s =>
        s + Metrics.Delete(latency = measurements.replicationLatency, actions = measurements.records)
      }
    }

    def purge(measurements: Measurements) = {
      StateT { s =>
        s + Metrics.Purge(latency = measurements.replicationLatency, actions = measurements.records)
      }
    }

    def round(duration: FiniteDuration, records: Int) = {
      StateT { s =>
        s + Metrics.Round(duration = duration, records = records)
      }
    }
  }

  val topicReplicator: StateT[Unit] = {
    implicit val F: Concurrent[StateT] = TestTemporal.temporal[State]

    val millis = timestamp.toEpochMilli + replicationLatency.toMillis
    implicit val fail = Fail.lift[StateT]
    implicit val fromTry = FromTry.lift[StateT]
    implicit val fromAttempt = FromAttempt.lift[StateT]
    implicit val fromJsResult = FromJsResult.lift[StateT]

    implicit val clock = Clock.const[StateT](nanos = 0, millis = millis)
    implicit val sleep = new Sleep[StateT] {
      def sleep(time: FiniteDuration): StateT[Unit] = ().pure[StateT]
      def applicative: Applicative[StateT] = Applicative[StateT]
      def monotonic: StateT[FiniteDuration] = clock.monotonic
      def realTime: StateT[FiniteDuration] = clock.realTime
    }

    implicit val measureDuration = MeasureDuration.fromClock(Clock[StateT])
    val kafkaRead = KafkaRead.summon[StateT, Payload]
    val eventualWrite = EventualWrite.summon[StateT, Payload]

    TopicReplicator.of[StateT, Payload](
      topic = topic,
      consumer = consumer.pure[StateT].toResource,
      consRecordToActionRecord = ConsRecordToActionRecord[StateT],
      kafkaRead = kafkaRead,
      eventualWrite = eventualWrite,
      journal = replicatedJournal,
      metrics = metrics,
      log = Log.empty[StateT],
      cacheOf = CacheOf.empty[StateT],
      replicatedOffsetNotifier = replicatedOffsetNotifier,
    )
  }

  final case class State(
    topics: List[Topic] = Nil,
    commits: List[Nem[Int, Long]] = Nil,
    records: List[ConsRecords] = Nil,
    pointers: Map[Topic, Map[Int, Long]] = Map.empty,
    replicatedOffsetNotifications: Map[Topic, Map[Int, Vector[Long]]] = Map.empty,
    journal: Map[String, List[EventRecord[EventualPayloadAndType]]] = Map.empty,
    metaJournal: Map[String, MetaJournal] = Map.empty,
    metrics: List[Metrics] = Nil,
  ) { self =>

    def +(metrics: Metrics): (State, Unit) = {
      val result = copy(metrics = metrics :: self.metrics)
      (result, ())
    }

    def subscribe(topic: Topic): State = {
      copy(topics = topic :: topics)
    }

    def delete(
      id: String,
      deleteTo: DeleteTo,
      partitionOffset: PartitionOffset,
      origin: Option[Origin],
    ): State = {

      def journal = self.journal.getOrElse(id, Nil)

      def delete(deleteTo: DeleteTo) = journal.dropWhile { _.seqNr <= deleteTo.value }

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
          deleteTo = result.map { _.toDeleteTo } orElse deleteTo1,
          expireAfter = none,
          origin = origin,
        )
        copy(journal = self.journal.updated(id, records), metaJournal = self.metaJournal.updated(id, metaJournal))
      }
    }

    def addReplicatedOffsetNotification(topicPartition: TopicPartition, offset: Long): State = {
      val topic = topicPartition.topic
      val partition = topicPartition.partition.value
      val prevPartitionToOffsets = replicatedOffsetNotifications.getOrElse(topic, Map.empty)
      val prevOffsets = prevPartitionToOffsets.getOrElse(partition, Vector.empty)
      copy(
        replicatedOffsetNotifications = replicatedOffsetNotifications.updated(
          topic,
          prevPartitionToOffsets.updated(
            partition,
            prevOffsets :+ offset,
          ),
        ),
      )
    }
  }

  type StateT[A] = cats.data.StateT[IO, State, A]

  object StateT {

    def apply[A](f: State => (State, A)): StateT[A] = cats.data.StateT[IO, State, A](a => IO.delay(f(a)))

    def unit(f: State => State): StateT[Unit] = apply { state =>
      val state1 = f(state)
      (state1, ())
    }
  }

  final case class MetaJournal(
    offset: PartitionOffset,
    deleteTo: Option[DeleteTo],
    expireAfter: Option[ExpireAfter],
    origin: Option[Origin],
  )

  case object NotImplemented extends RuntimeException with NoStackTrace
}
