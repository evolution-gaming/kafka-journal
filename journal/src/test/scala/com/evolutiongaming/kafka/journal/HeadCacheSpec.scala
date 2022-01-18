package com.evolutiongaming.kafka.journal

import java.time.Instant
import cats.data.{NonEmptyList => Nel, NonEmptyMap => Nem, NonEmptySet => Nes}
import cats.effect.{Concurrent, IO, Ref, Resource, Temporal}
import cats.effect.syntax.resource._
import cats.syntax.all._
import com.evolutiongaming.catshelper.Log
import com.evolutiongaming.kafka.journal.eventual.TopicPointers
import com.evolutiongaming.kafka.journal.IOSuite._
import com.evolutiongaming.kafka.journal.conversions.{ActionToProducerRecord, KafkaWrite}
import com.evolutiongaming.skafka._
import com.evolutiongaming.catshelper.CatsHelper._
import com.evolutiongaming.smetrics.CollectorRegistry
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec
import TestJsonCodec.instance
import com.evolutiongaming.retry.Sleep
import com.evolutiongaming.skafka.consumer.{ConsumerRecord, ConsumerRecords}

import scala.collection.immutable.Queue
import scala.concurrent.duration._
import scala.util.Try
import scala.util.control.NoStackTrace

class HeadCacheSpec extends AsyncWordSpec with Matchers {
  import HeadCacheSpec._

  "HeadCache" should {

    "return result, records are in cache" in {
      val offsetLast = Offset.unsafe(10)

      val eventual = HeadCache.Eventual.empty[IO]

      val key = Key(id = "id", topic = topic)
      val records = ConsumerRecordsOf {
        for {
          idx   <- (0L to offsetLast.value).toList
          seqNr <- SeqNr.opt(idx + 1)
        } yield {
          val action = appendOf(key, seqNr)
          consumerRecordOf(action, topicPartition, Offset.unsafe(idx))
        }
      }

      val state = TestConsumer.State(
        topics = Map((topic, List(partition))),
        records = Queue(records.pure[Try]))

      val result = for {
        stateRef <- Ref[IO].of(state)
        consumer  = TestConsumer.of(stateRef)
        _        <- headCacheOf(eventual, consumer).use { headCache =>
          for {
            result <- headCache.get(key = key, partition = partition, offset = offsetLast)
            _       = result shouldEqual HeadInfo.append(SeqNr.unsafe(11), none, Offset.min).asRight
            state  <- stateRef.get
          } yield {
            state shouldEqual TestConsumer.State(
              actions = List(
                TestConsumer.Action.Seek(topic, Nem.of((partition, Offset.min))),
                TestConsumer.Action.Assign(topic, Nes.of(partition))),
              topics = Map((topic, List(partition))))
          }
        }
      } yield {}

      result.run()
    }

    "return result, all events are already replicated and cache is empty" in {
      val marker = Offset.unsafe(10)

      val pointers = Map((partition, marker))
      implicit val eventual = HeadCache.Eventual.const(TopicPointers(pointers).pure[IO])

      val state = TestConsumer.State(
        topics = Map((topic, List(partition))))

      val key = Key(id = "id", topic = topic)

      val result = for {
        stateRef <- Ref[IO].of(state)
        consumer  = TestConsumer.of(stateRef)
        _        <- headCacheOf(eventual, consumer).use { headCache =>
          for {
            a <- headCache.get(key = key, partition = partition, offset = marker)
            _  = a shouldEqual HeadInfo.empty.asRight
          } yield {}
        }
      } yield {}

      result.run()
    }

    "return result, after events are replicated" in {
      val marker = Offset.unsafe(100)

      val eventual = HeadCache.Eventual.empty[IO]

      val state = TestConsumer.State.empty

      val key = Key(id = "id", topic = topic)
      val result = for {
        stateRef <- Ref[IO].of(state)
        consumer  = TestConsumer.of(stateRef)
        _        <- headCacheOf(eventual, consumer).use { headCache =>
          for {
            result <- Concurrent[IO].start { headCache.get(key = key, partition = partition, offset = marker) }
            _      <- stateRef.update { _.copy(topics = Map((topic, List(partition)))) }
            _      <- stateRef.update { state =>
              val action = Action.Mark(key, timestamp, ActionHeader.Mark("mark", none, Version.current.some))
              val record = consumerRecordOf(action, topicPartition, marker)
              val records = ConsumerRecordsOf(List(record))
              state.enqueue(records.pure[Try])
            }
            result <- result.join
            _       = result shouldEqual HeadInfo.empty.asRight
            state  <- stateRef.get
          } yield {
            state shouldEqual TestConsumer.State(
              actions = List(
                TestConsumer.Action.Seek(topic, Nem.of((partition, Offset.min))),
                TestConsumer.Action.Assign(topic, Nes.of(partition))),
              topics = Map((topic, List(partition))))
          }
        }
      } yield {}

      result.run()
    }

    "clean cache after events are being replicated" ignore {
      val key = Key(id = "id", topic = topic)

      val offsetLast = Offset.unsafe(10)
      val records = for {
        offset <- (0L until offsetLast.value).toList
        seqNr  <- SeqNr.opt(offset + 1)
      } yield {
        val action = appendOf(key, seqNr)
        val record = consumerRecordOf(action, topicPartition, Offset.unsafe(offset))
        ConsumerRecordsOf(List(record)).pure[Try]
      }

      val state = TestConsumer.State(
        topics = Map((topic, List(Partition.min))),
        records = Queue(records: _*))

      val result = for {
        pointers  <- Ref.of[IO, Map[Partition, Offset]](Map.empty)
        stateRef  <- Ref[IO].of(state)
        consumer   = TestConsumer.of(stateRef)
        headCache  = {
          val topicPointers = for {
            pointers <- pointers.get
          } yield TopicPointers(pointers)
          val eventual = HeadCache.Eventual.const(topicPointers)
          headCacheOf(eventual, consumer)
        }
        _         <- headCache.use { headCache =>
          for {
            result <- headCache.get(
              key = key,
              partition = partition,
              offset = offsetLast)
            _ <- pointers.update { pointers => pointers ++ Map((partition, offsetLast)) }
          } yield {
            state shouldEqual TestConsumer.State(
              actions = List(
                TestConsumer.Action.Seek(topic, Nem.of((partition, Offset.min))),
                TestConsumer.Action.Assign(topic, Nes.of(partition))),
              topics = Map((topic, List(partition))))

            result shouldEqual HeadInfo.empty.some
          }
        }

      } yield {}

      result.run()
    }

    "invalidate cache in case exceeding maxSize" in {
      val state = TestConsumer.State(
        topics = Map((topic, List(Partition.min))))

      val config = HeadCacheSpec.config.copy(maxSize = 1)

      val result = for {
        pointers  <- Ref.of[IO, Map[Partition, Offset]](Map.empty)
        stateRef  <- Ref[IO].of(state)
        consumer   = TestConsumer.of(stateRef)
        headCache  = {
          val topicPointers = for {
            pointers <- pointers.get
          } yield {
            TopicPointers(pointers)
          }
          val eventual = HeadCache.Eventual.const(topicPointers)
          headCacheOf(eventual, consumer, config)
        }
        _         <- headCache.use { headCache =>

          val key0 = Key(id = "id0", topic = topic)
          val key1 = Key(id = "id1", topic = topic)
          val enqueue = (key: Key, offset: Offset) => {
            stateRef.update { state =>
              val action = appendOf(key, SeqNr.min)
              val record = consumerRecordOf(action, topicPartition, offset)
              val records = ConsumerRecordsOf(List(record))
              state.enqueue(records.pure[Try])
            }
          }
          for {
            _     <- enqueue(key0, Offset.min)
            a     <- headCache.get(key0, partition, Offset.min)
            _      = a shouldEqual HeadInfo.append(SeqNr.min, none, Offset.min).asRight
            _     <- enqueue(key1, Offset.unsafe(1))
            a     <- headCache.get(key0, partition, Offset.unsafe(1))
            _      = a shouldEqual HeadCacheError.invalid.asLeft
            a     <- headCache.get(key1, partition, Offset.unsafe(1))
            _      = a shouldEqual HeadCacheError.invalid.asLeft
            _     <- pointers.update { _ ++ Map((partition, Offset.unsafe(1))) }
            a     <- headCache.get(key1, partition, Offset.unsafe(1))
            _      = a shouldEqual HeadCacheError.invalid.asLeft
            _     <- enqueue(key0, Offset.unsafe(2))
            a     <- headCache.get(key0, partition, Offset.unsafe(2))
            _      = a shouldEqual HeadInfo.append(SeqNr.min, none, Offset.unsafe(2)).asRight
            state <- stateRef.get
          } yield {
            state shouldEqual TestConsumer.State(
              actions = List(
                TestConsumer.Action.Seek(topic, Nem.of((partition, Offset.min))),
                TestConsumer.Action.Assign(topic, Nes.of(partition))),
              topics = Map((topic, List(partition))))
          }
        }
      } yield {}

      result.run()
    }

    "retry in case consuming failed" in {
      val state = TestConsumer.State(
        topics = Map((topic, List(Partition.min))))

      val result = for {
        pointers  <- Ref.of[IO, Map[Partition, Offset]](Map.empty)
        stateRef  <- Ref[IO].of(state)
        consumer   = TestConsumer.of(stateRef)
        headCache  = {
          val topicPointers = for {
            pointers <- pointers.get
          } yield TopicPointers(pointers)
          val eventual = HeadCache.Eventual.const(topicPointers)
          headCacheOf(eventual, consumer)
        }
        _         <- headCache.use { headCache =>

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
            _     <- enqueue(Offset.min)
            a     <- headCache.get(key, partition, Offset.min)
            _      = a shouldEqual HeadInfo.append(SeqNr.min, none, Offset.min).asRight
            _     <- stateRef.update { _.enqueue(TestError.raiseError[Try, ConsumerRecords[String, Unit]]) }
            _     <- enqueue(Offset.unsafe(1))
            a     <- headCache.get(key, partition, Offset.unsafe(1))
            _      = a shouldEqual HeadInfo.append(SeqNr.min, none, Offset.min).asRight
            state <- stateRef.get
          } yield {
            state shouldEqual TestConsumer.State(
              actions = List(
                TestConsumer.Action.Seek(topic, Nem.of((partition, Offset.unsafe(1)))),
                TestConsumer.Action.Assign(topic, Nes.of(partition)),
                TestConsumer.Action.Release,
                TestConsumer.Action.Seek(topic, Nem.of((partition, Offset.min))),
                TestConsumer.Action.Assign(topic, Nes.of(partition))),
              topics = Map((topic, List(partition))))
          }
        }
      } yield {}

      result.run()
    }

    "timeout" in {
      val consumer   = HeadCache.Consumer.empty[IO]
      val headCache  = headCacheOf(
        HeadCache.Eventual.empty, 
        consumer.pure[IO].toResource,
        config.copy(timeout = 10.millis))
      val result = headCache.use { headCache =>
        val key = Key(id = "id", topic = topic)
        for {
          a <- headCache.get(key, partition, Offset.min).timeout(1.second)
          _  = a shouldEqual HeadCacheError.timeout(10.millis).asLeft
        } yield {}
      }

      result.run()
    }

    // TODO headcache: how to test?
    "do not leak on cancel" ignore {
      val consumer   = HeadCache.Consumer.empty[IO]
      val headCache  = headCacheOf(
        HeadCache.Eventual.empty,
        consumer.pure[IO].toResource)
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
      val consumer   = HeadCache.Consumer.empty[IO]
      val headCache  = headCacheOf(
        HeadCache.Eventual.empty,
        consumer.pure[IO].toResource)
      val result = for {
        a <- headCache.use { headCache =>
          val key = Key(id = "id", topic = topic)
          for {
            a <- headCache.get(key, partition, Offset.min).startEnsure
            _ <- Sleep[IO].sleep(100.millis)
          } yield a
        }
        a <- a.join.attempt
        _  = a shouldEqual HeadCacheReleasedError.asLeft
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
  val config: HeadCacheConfig = HeadCacheConfig(cleanInterval = 100.millis)

  val recordMetadata: HeaderMetadata = HeaderMetadata.empty

  val headers: Headers = Headers.empty

  def consumerRecordOf(
    action: Action,
    topicPartition: TopicPartition,
    offset: Offset)(implicit
    actionToProducerRecord: ActionToProducerRecord[Try]
  ): ConsumerRecord[String, Unit] = {
    ConsumerRecordOf[Try](action, topicPartition, offset)
      .get
      .void
  }

  def appendOf(key: Key, seqNr: SeqNr): Action.Append  = {
    implicit val kafkaWrite = KafkaWrite.summon[Try, Payload]
    Action.Append.of[Try, Payload](
      key = key,
      timestamp = timestamp,
      origin = none,
      version = Version.current.some,
      events = Events(Nel.of(Event(seqNr)), PayloadMetadata.empty),
      metadata = recordMetadata,
      headers = headers).get
  }

  
  implicit val LogIO: Log[IO] = Log.empty[IO]


  def headCacheOf(
    eventual: HeadCache.Eventual[IO],
    consumer: Resource[IO, HeadCache.Consumer[IO]],
    config: HeadCacheConfig = config
  ): Resource[IO, HeadCache[IO]] = {

    for {
      metrics   <- HeadCacheMetrics.of[IO](CollectorRegistry.empty)
      headCache <- HeadCache.of[IO](
        log = LogIO,
        config = config,
        eventual = eventual,
        consumer = consumer,
        metrics = metrics.some)
    } yield headCache
  }


  object TestConsumer {

    def of(stateRef: Ref[IO, State]): Resource[IO, HeadCache.Consumer[IO]] = {
      val consumer = apply(stateRef)
      val release = stateRef.update { _.append(Action.Release) }
      Resource((consumer, release).pure[IO])
    }

    def apply(stateRef: Ref[IO, State]): HeadCache.Consumer[IO] = {
      new HeadCache.Consumer[IO] {

        def assign(topic: Topic, partitions: Nes[Partition]) = {
          stateRef.update { _.append(Action.Assign(topic, partitions)) }
        }

        def seek(topic: Topic, offsets: Nem[Partition, Offset]) = {
          stateRef.update { _.append(Action.Seek(topic, offsets)) }
        }

        val poll = {
          for {
            _       <- Sleep[IO].sleep(1.milli)
            records <- stateRef.modify { state =>
              state.records.dequeueOption match {
                case None                    => (state, ConsumerRecords.empty[String, Unit].pure[Try])
                case Some((record, records)) =>
                  val stateUpdated = state.copy(records = records)
                  (stateUpdated, record)
              }
            }
            records <- IO.fromTry(records)
          } yield records
        }

        def partitions(topic: Topic) = {
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
      records: Queue[Try[ConsumerRecords[String, Unit]]] = Queue.empty)

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