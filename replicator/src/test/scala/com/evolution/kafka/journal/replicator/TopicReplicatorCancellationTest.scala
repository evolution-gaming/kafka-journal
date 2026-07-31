package com.evolution.kafka.journal.replicator

import cats.data.NonEmptyList as Nel
import cats.effect.unsafe.implicits.global
import cats.effect.{Clock, Deferred, IO, Ref, Resource}
import cats.syntax.all.*
import com.evolution.kafka.journal.*
import com.evolution.kafka.journal.conversions.{ConsRecordToActionRecord, KafkaRead}
import com.evolution.kafka.journal.eventual.*
import com.evolutiongaming.catshelper.{Log, MeasureDuration}
import com.evolutiongaming.skafka.*
import com.evolutiongaming.skafka.consumer.{ConsumerRecord, RebalanceListener1, WithSize}
import com.evolutiongaming.sstream.Stream
import org.scalatest.funsuite.AsyncFunSuite
import org.scalatest.matchers.should.Matchers
import scodec.bits.ByteVector

import java.time.Instant
import scala.collection.immutable.SortedSet
import scala.concurrent.duration.*
import scala.util.control.NoStackTrace

class TopicReplicatorCancellationTest extends AsyncFunSuite with Matchers {

  import TopicReplicatorCancellationTest.*

  test("key failure does not cancel other keys of the same partition") {
    val result = for {
      started <- Deferred[IO, Unit]
      finished <- Deferred[IO, Unit]
      cancelled <- Deferred[IO, Unit]
      deletes = Map(
        (
          "ok",
          (started.complete(()) *> IO.sleep(50.millis) *> finished.complete(()).as(true))
            .onCancel(cancelled.complete(()).void),
        ),
        ("bad", started.get *> IO.raiseError[Boolean](Error)),
      )
      records = Map((Partition.min, Nel.of(record("ok", 0, 0), record("bad", 0, 1))))
      fiber <- consume(records, deletes).start
      _ <- finished.get.timeout(5.seconds)
      wasCancelled <- cancelled.tryGet
      _ <- fiber.cancel
    } yield {
      wasCancelled shouldEqual none
    }
    result.unsafeToFuture()
  }

  test("key failure does not cancel keys of other partitions") {
    val result = for {
      started <- Deferred[IO, Unit]
      finished <- Deferred[IO, Unit]
      cancelled <- Deferred[IO, Unit]
      deletes = Map(
        (
          "ok",
          (started.complete(()) *> IO.sleep(50.millis) *> finished.complete(()).as(true))
            .onCancel(cancelled.complete(()).void),
        ),
        ("bad", started.get *> IO.raiseError[Boolean](Error)),
      )
      records = Map(
        (Partition.unsafe(0), Nel.of(record("bad", 0, 0))),
        (Partition.unsafe(1), Nel.of(record("ok", 1, 0))),
      )
      fiber <- consume(records, deletes).start
      wasFinished <- finished.get.timeout(1.second).attempt
      wasCancelled <- cancelled.tryGet
      _ <- fiber.cancel
    } yield {
      wasCancelled shouldEqual none
      wasFinished shouldEqual ().asRight
    }
    pendingUntilFixed {
      result.unsafeRunSync()
      ()
    }
  }
}

object TopicReplicatorCancellationTest {

  val topic: Topic = "topic"

  val Error: Throwable = new RuntimeException("test") with NoStackTrace

  implicit val measureDuration: MeasureDuration[IO] = MeasureDuration.fromClock(Clock[IO])

  val consRecordToActionRecord: ConsRecordToActionRecord[IO] = { (record: ConsRecord) =>
    {
      record
        .key
        .traverse { key =>
          val action = Action.delete(
            key = Key(id = key.value, topic = record.topic),
            timestamp = Instant.EPOCH,
            header = ActionHeader.Delete(SeqNr.min.toDeleteTo, none, none),
          )
          val partitionOffset = PartitionOffset(record.topicPartition.partition, record.offset)
          ActionRecord(action, partitionOffset).pure[IO]
        }
    }
  }

  val kafkaRead: KafkaRead[IO, Unit] = { (_: PayloadAndType) =>
    IO.raiseError(new IllegalStateException("kafkaRead is not expected"))
  }

  val eventualWrite: EventualWrite[IO, Unit] = { (_: Unit) =>
    IO.raiseError(new IllegalStateException("eventualWrite is not expected"))
  }

  def journalOf(deletes: Map[String, IO[Boolean]]): ReplicatedJournal[IO] = {
    new ReplicatedJournal[IO] {

      def topics: IO[SortedSet[Topic]] = SortedSet.empty[Topic].pure[IO]

      def journal(topic: Topic): Resource[IO, ReplicatedTopicJournal[IO]] = {
        val topicJournal = new ReplicatedTopicJournal[IO] {

          def apply(partition: Partition): Resource[IO, ReplicatedPartitionJournal[IO]] = {
            val partitionJournal = new ReplicatedPartitionJournal[IO] {

              def offsets: ReplicatedPartitionJournal.Offsets[IO] = {
                new ReplicatedPartitionJournal.Offsets[IO] {

                  def get: IO[Option[Offset]] = none[Offset].pure[IO]

                  def create(offset: Offset, timestamp: Instant): IO[Unit] = IO.unit

                  def update(offset: Offset, timestamp: Instant): IO[Unit] = IO.unit
                }
              }

              def journal(id: String): Resource[IO, ReplicatedKeyJournal[IO]] = {
                val keyJournal = new ReplicatedKeyJournal[IO] {

                  def append(
                    offset: Offset,
                    timestamp: Instant,
                    expireAfter: Option[ExpireAfter],
                    events: Nel[EventRecord[EventualPayloadAndType]],
                  ): IO[Boolean] = {
                    IO.raiseError(new IllegalStateException("append is not expected"))
                  }

                  def delete(
                    offset: Offset,
                    timestamp: Instant,
                    deleteTo: DeleteTo,
                    origin: Option[Origin],
                  ): IO[Boolean] = {
                    deletes.getOrElse(id, IO.raiseError(new IllegalStateException(s"unexpected key: $id")))
                  }

                  def purge(
                    offset: Offset,
                    timestamp: Instant,
                  ): IO[Boolean] = {
                    IO.raiseError(new IllegalStateException("purge is not expected"))
                  }
                }
                Resource.pure(keyJournal)
              }
            }
            Resource.pure(partitionJournal)
          }
        }
        Resource.pure(topicJournal)
      }
    }
  }

  def consumerOf(records: Ref[IO, Option[Map[Partition, Nel[ConsRecord]]]]): TopicConsumer[IO] = {
    new TopicConsumer[IO] {

      def subscribe(listener: RebalanceListener1[IO]): IO[Unit] = IO.unit

      def poll: Stream[IO, Map[Partition, Nel[ConsRecord]]] = {
        val poll = records
          .modify {
            case Some(records) => (none, records.pure[IO])
            case None => (none, IO.never)
          }
          .flatten
        Stream.repeat(poll)
      }

      def commit: TopicCommit[IO] = TopicCommit.empty
    }
  }

  def consume(
    records: Map[Partition, Nel[ConsRecord]],
    deletes: Map[String, IO[Boolean]],
  ): IO[Unit] = {
    for {
      records <- Ref[IO].of(records.some)
      result <- TopicReplicator.of(
        topic = topic,
        consumer = Resource.pure[IO, TopicConsumer[IO]](consumerOf(records)),
        consRecordToActionRecord = consRecordToActionRecord,
        kafkaRead = kafkaRead,
        eventualWrite = eventualWrite,
        journal = journalOf(deletes),
        metrics = TopicReplicatorMetrics.empty[IO],
        log = Log.empty[IO],
        cacheOf = CacheOf.empty[IO],
        replicatedOffsetNotifier = ReplicatedOffsetNotifier.empty[IO],
      )
    } yield result
  }

  def record(key: String, partition: Int, offset: Long): ConsRecord = {
    ConsumerRecord[String, ByteVector](
      topicPartition = TopicPartition(topic, Partition.unsafe(partition)),
      offset = Offset.unsafe(offset),
      timestampAndType = none,
      key = WithSize(key).some,
    )
  }
}
