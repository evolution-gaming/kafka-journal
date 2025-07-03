package com.evolution.kafka.journal

import cats.data.{NonEmptyList as Nel, NonEmptyMap as Nem, NonEmptySet as Nes}
import cats.effect.*
import cats.syntax.all.*
import com.evolution.kafka.journal.IOSuite.*
import com.evolution.kafka.journal.TestJsonCodec.instance
import com.evolution.kafka.journal.conversions.ActionToProducerRecord
import com.evolution.kafka.journal.eventual.TopicPointers
import com.evolutiongaming.catshelper.CatsHelper.*
import com.evolutiongaming.catshelper.Log
import com.evolutiongaming.retry.Sleep
import com.evolutiongaming.skafka.*
import com.evolutiongaming.skafka.consumer.{ConsumerRecord, ConsumerRecords}
import com.evolutiongaming.smetrics.CollectorRegistry
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec

import java.time.Instant
import scala.collection.immutable.Queue
import scala.concurrent.duration.*
import scala.util.Try
import scala.util.control.NoStackTrace

class HeadCacheSpec extends AsyncWordSpec with Matchers {
  import HeadCacheSpec.*

  "HeadCache" should {

    "return result, records are in cache" in {
      val offsetLast = Offset.unsafe(10)

      val eventual = HeadCache.Eventual.empty[IO]

      val key = Key(id = "id", topic = topic)
      val records = ConsumerRecordsOf {
        for {
          idx <- (0L to offsetLast.value).toList
          seqNr <- SeqNr.opt(idx + 1)
        } yield {
          val action = appendOf(key, seqNr)
          consumerRecordOf(action, topicPartition, Offset.unsafe(idx))
        }
      }

      val state = TestConsumer.State(topics = Map((topic, List(partition))), records = Queue(records.pure[Try]))

      val result = for {
        stateRef <- Ref[IO].of(state)
        consumer = TestConsumer.make(stateRef)
        _ <- headCacheOf(eventual, consumer).use { headCache =>
          for {
            result <- headCache.get(key = key, partition = partition, offset = offsetLast)
            _ = result shouldEqual HeadInfo.append(Offset.min, SeqNr.unsafe(11), none).some
            state <- stateRef.get
          } yield {
            state shouldEqual TestConsumer.State(
              actions = List(
                TestConsumer.Action.Seek(topic, Nem.of((partition, Offset.min))),
                TestConsumer.Action.Assign(topic, Nes.of(partition)),
                TestConsumer.Action.Release,
              ),
              topics = Map((topic, List(partition))),
            )
          }
        }
      } yield {}

      result.run()
    }

    "return result, all events are already replicated and cache is empty" in {
      val marker = Offset.unsafe(10)

      val pointers = Map((partition, marker))
      implicit val eventual: HeadCache.Eventual[IO] = HeadCache.Eventual.const(TopicPointers(pointers).pure[IO])

      val state = TestConsumer.State(topics = Map((topic, List(partition))))

      val key = Key(id = "id", topic = topic)

      val result = for {
        stateRef <- Ref[IO].of(state)
        consumer = TestConsumer.make(stateRef)
        _ <- headCacheOf(eventual, consumer).use { headCache =>
          for {
            a <- headCache.get(key = key, partition = partition, offset = marker)
            _ = a shouldEqual HeadInfo.empty.some
          } yield {}
        }
      } yield {}

      result.run()
    }

    "return result, after events are replicated" in {
      val marker = Offset.unsafe(100)

      val eventual = HeadCache.Eventual.empty[IO]

      val state = TestConsumer.State(topics = Map((topic, List(partition))))

      val key = Key(id = "id", topic = topic)
      val result = for {
        stateRef <- Ref[IO].of(state)
        consumer = TestConsumer.make(stateRef)
        _ <- headCacheOf(eventual, consumer).use { headCache =>
          for {
            result <- Concurrent[IO].start { headCache.get(key = key, partition = partition, offset = marker) }
            _ <- stateRef.update { _.copy(topics = Map((topic, List(partition)))) }
            _ <- stateRef.update { state =>
              val action = Action.Mark(key, timestamp, ActionHeader.Mark("mark", none, Version.current.some))
              val record = consumerRecordOf(action, topicPartition, marker)
              val records = ConsumerRecordsOf(List(record))
              state.enqueue(records.pure[Try])
            }
            result <- result.join
            _ = result shouldEqual Outcome.succeeded(IO.pure(HeadInfo.empty.some))
            state <- stateRef.get
          } yield {
            state shouldEqual TestConsumer.State(
              actions = List(
                TestConsumer.Action.Seek(topic, Nem.of((partition, Offset.min))),
                TestConsumer.Action.Assign(topic, Nes.of(partition)),
                TestConsumer.Action.Release,
              ),
              topics = Map((topic, List(partition))),
            )
          }
        }
      } yield {}

      result.run()
    }

    "retry in case consuming failed" in {
      val state = TestConsumer.State(topics = Map((topic, List(Partition.min))))

      val result = for {
        pointers <- Ref.of[IO, Map[Partition, Offset]](Map.empty)
        stateRef <- Ref[IO].of(state)
        consumer = TestConsumer.make(stateRef)
        headCache = {
          val topicPointers = for {
            pointers <- pointers.get
          } yield TopicPointers(pointers)
          val eventual = HeadCache.Eventual.const(topicPointers)
          headCacheOf(eventual, consumer)
        }
        _ <- headCache.use { headCache =>
          val key = Key(id = "id", topic = topic)
          val enqueue = (offset: Offset) => {
            stateRef.update { state =>
              val action = appendOf(key, SeqNr.min)
              val record = consumerRecordOf(action, topicPartition, offset)
              val records = ConsumerRecordsOf(List(record))
              state.enqueue(records.pure[Try])
            }
          }
          for {
            _ <- enqueue(Offset.min)
            a <- headCache.get(key, partition, Offset.min)
            _ = a shouldEqual HeadInfo.append(Offset.min, SeqNr.min, none).some
            _ <- stateRef.update { _.enqueue(TestError.raiseError[Try, ConsumerRecords[String, Unit]]) }
            _ <- enqueue(Offset.unsafe(1))
            a <- headCache.get(key, partition, Offset.unsafe(1))
            _ = a shouldEqual HeadInfo.append(Offset.min, SeqNr.min, none).some
            state <- stateRef.get
          } yield {
            state shouldEqual TestConsumer.State(
              actions = List(
                TestConsumer.Action.Seek(topic, Nem.of((partition, Offset.unsafe(1)))),
                TestConsumer.Action.Assign(topic, Nes.of(partition)),
                TestConsumer.Action.Release,
                TestConsumer.Action.Seek(topic, Nem.of((partition, Offset.min))),
                TestConsumer.Action.Assign(topic, Nes.of(partition)),
                TestConsumer.Action.Release,
              ),
              topics = Map((topic, List(partition))),
            )
          }
        }
      } yield {}

      result.run()
    }

    "timeout" in {
      val headCache =
        headCacheOf(HeadCache.Eventual.empty, consumerEmpty.pure[IO].toResource, config.copy(timeout = 10.millis))
      val result = headCache.use { headCache =>
        val key = Key(id = "id", topic = topic)
        for {
          a <- headCache.get(key, partition, Offset.min).timeout(1.second)
          _ = a shouldEqual none
        } yield {}
      }

      result.run()
    }

    // TODO headcache: how to test?
    "do not leak on cancel" ignore {
      val consumer = TopicCache.Consumer.empty[IO]
      val headCache = headCacheOf(HeadCache.Eventual.empty, consumer.pure[IO].toResource)
      val result = headCache.use { headCache =>
        val key = Key(id = "id", topic = topic)
        for {
          a <- headCache.get(key, partition, Offset.min).startEnsure
          _ <- Temporal[IO].sleep(1.second)
          _ <- a.cancel
          _ <- Temporal[IO].sleep(3.seconds)
        } yield {}
      }

      result.run(10.seconds)
    }

    "not leak resources on release" in {
      val headCache = headCacheOf(HeadCache.Eventual.empty, consumerEmpty.pure[IO].toResource)
      val result = for {
        a <- headCache.use { headCache =>
          val key = Key(id = "id", topic = topic)
          for {
            a <- headCache.get(key, partition, Offset.min).startEnsure
            _ <- Sleep[IO].sleep(100.millis)
          } yield a
        }
        a <- a.join.attempt
        _ = a shouldEqual Right(Outcome.errored(ReleasedError))
      } yield {}
      result.run()
    }
  }
}

object HeadCacheSpec {
  val timestamp: Instant = Instant.now()
  val topic: Topic = "topic"
  val partition: Partition = Partition.min
  val topicPartition: TopicPartition = TopicPartition(topic = topic, partition = partition)
  val config: HeadCacheConfig = HeadCacheConfig(removeInterval = 100.millis)

  val recordMetadata: HeaderMetadata = HeaderMetadata.empty

  val headers: Headers = Headers.empty

  def consumerRecordOf(
    action: Action,
    topicPartition: TopicPartition,
    offset: Offset,
  )(implicit
    actionToProducerRecord: ActionToProducerRecord[Try],
  ): ConsumerRecord[String, Unit] = {
    ConsumerRecordOf[Try](action, topicPartition, offset)
      .get
      .void
  }

  def appendOf(key: Key, seqNr: SeqNr): Action.Append = {
    Action
      .Append
      .of[Try, Payload](
        key = key,
        timestamp = timestamp,
        origin = none,
        version = Version.current.some,
        events = Events(Nel.of(Event(seqNr)), PayloadMetadata.empty),
        metadata = recordMetadata,
        headers = headers,
      )
      .get
  }

  implicit val LogIO: Log[IO] = Log.empty[IO]

  val consumerEmpty: TopicCache.Consumer[IO] = new TopicCache.Consumer[IO] {
    def assign(topic: Topic, partitions: Nes[Partition]): IO[Unit] = IO.unit
    def seek(topic: Topic, offsets: Nem[Partition, Offset]): IO[Unit] = IO.unit
    def poll: IO[ConsumerRecords[Tag, Unit]] = ConsumerRecords.empty[String, Unit].pure[IO]
    def partitions(topic: Topic): IO[Set[Partition]] = Set(partition).pure[IO]
  }

  def headCacheOf(
    eventual: HeadCache.Eventual[IO],
    consumer: Resource[IO, TopicCache.Consumer[IO]],
    config: HeadCacheConfig = config,
  ): Resource[IO, HeadCache[IO]] = {

    for {
      metrics <- HeadCacheMetrics.make[IO](CollectorRegistry.empty)
      headCache <- HeadCache
        .make[IO](log = LogIO, config = config, eventual = eventual, consumer = consumer, metrics = metrics.some)
    } yield headCache
  }

  object TestConsumer {

    def make(stateRef: Ref[IO, State]): Resource[IO, TopicCache.Consumer[IO]] = {
      val consumer = apply(stateRef)
      val release = stateRef.update { _.append(Action.Release) }
      Resource((consumer, release).pure[IO])
    }

    def apply(stateRef: Ref[IO, State]): TopicCache.Consumer[IO] = {
      new TopicCache.Consumer[IO] {

        def assign(topic: Topic, partitions: Nes[Partition]): IO[Unit] = {
          stateRef.update { _.append(Action.Assign(topic, partitions)) }
        }

        def seek(topic: Topic, offsets: Nem[Partition, Offset]): IO[Unit] = {
          stateRef.update { _.append(Action.Seek(topic, offsets)) }
        }

        val poll: IO[ConsumerRecords[Tag, Unit]] = {
          for {
            _ <- Sleep[IO].sleep(1.milli)
            records <- stateRef.modify { state =>
              state.records.dequeueOption match {
                case None => (state, ConsumerRecords.empty[String, Unit].pure[Try])
                case Some((record, records)) =>
                  val stateUpdated = state.copy(records = records)
                  (stateUpdated, record)
              }
            }
            records <- IO.fromTry(records)
          } yield records
        }

        def partitions(topic: Topic): IO[Set[Partition]] = {
          for {
            state <- stateRef.get
          } yield {
            state.topics.get(topic).fold(Set.empty[Partition])(_.toSet)
          }
        }
      }
    }

    sealed abstract class Action

    object Action {

      final case class Assign(topic: Topic, partitions: Nes[Partition]) extends Action

      final case class Seek(topic: Topic, offsets: Nem[Partition, Offset]) extends Action

      case object Release extends Action

    }

    final case class State(
      actions: List[Action] = List.empty,
      topics: Map[Topic, List[Partition]] = Map.empty,
      records: Queue[Try[ConsumerRecords[String, Unit]]] = Queue.empty,
    )

    object State {

      val empty: State = State()

      implicit class StateOps(val self: State) extends AnyVal {

        def enqueue(records: Try[ConsumerRecords[String, Unit]]): State = {
          self.copy(records = self.records.enqueue(records))
        }

        def append(action: Action): State = self.copy(actions = action :: self.actions)
      }
    }
  }

  case object TestError extends RuntimeException with NoStackTrace
}
