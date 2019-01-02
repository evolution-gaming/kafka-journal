package com.evolutiongaming.kafka.journal

import java.time.Instant

import cats.effect.concurrent.Ref
import cats.effect.{Concurrent, IO, Resource, Timer}
import cats.implicits._
import com.evolutiongaming.kafka.journal.eventual.TopicPointers
import com.evolutiongaming.kafka.journal.util.IOSuite._
import com.evolutiongaming.nel.Nel
import com.evolutiongaming.skafka._
import com.evolutiongaming.skafka.consumer.ConsumerRecords
import org.scalatest.{AsyncWordSpec, Matchers}

import scala.collection.immutable.Queue
import scala.concurrent.duration._

class HeadCacheSpec extends AsyncWordSpec with Matchers {
  import HeadCacheSpec._

  "HeadCache" should {

    "return result, records are in cache" in {
      val offsetLast = 10l

      implicit val eventual = HeadCache.Eventual.empty[IO]

      val key = Key(id = "id", topic = topic)
      val records = ConsumerRecordsOf {
        for {
          idx <- (0l to offsetLast).toList
          seqNr <- SeqNr.opt(idx + 1)
        } yield {
          val action = Action.Append(key, timestamp, none, Nel(Event(seqNr)))
          ConsumerRecordOf(action, topicPartition, idx)
        }
      }

      val state = TestConsumer.State(
        topics = Map((topic, List(partition))),
        records = Queue(records))

      val result = for {
        ref <- Ref.of[IO, IO[TestConsumer.State]](state.pure[IO])
        consumer = TestConsumer(ref)
        headCache <- headCacheOf(consumer.pure[IO])
        result <- headCache(key = key, partition = partition, offset = offsetLast)
        state <- ref.get
        state <- state
        _ <- headCache.close
      } yield {
        state shouldEqual TestConsumer.State(
          assigns = List(TestConsumer.Assign(topic, Nel(partition))),
          seeks = List(TestConsumer.Seek(topic, Map((partition, 0)))),
          topics = Map((topic, List(partition))))

        result shouldEqual Some(HeadCache.Result(seqNr = Some(SeqNr(11)), deleteTo = none))
      }

      result.run()
    }

    "return result, all events are already replicated and cache is empty" in {
      val marker = 10l

      val pointers = Map((partition, marker))
      implicit val eventual = HeadCache.Eventual.const(TopicPointers(pointers).pure[IO])

      val state = TestConsumer.State(
        topics = Map((topic, List(partition))))

      val key = Key(id = "id", topic = topic)

      val result = for {
        ref <- Ref.of[IO, IO[TestConsumer.State]](state.pure[IO])
        consumer = TestConsumer(ref)
        headCache <- headCacheOf(consumer.pure[IO])
        result <- headCache(key = key, partition = partition, offset = marker)
        _ <- headCache.close
      } yield {
        result shouldEqual Some(HeadCache.Result(seqNr = none, deleteTo = none))
      }

      result.run()
    }

    "return result, after events are replicated" in {
      val marker = 100l

      implicit val eventual = HeadCache.Eventual.empty[IO]

      val state = TestConsumer.State.Empty

      val key = Key(id = "id", topic = topic)
      val result = for {
        ref <- Ref.of[IO, IO[TestConsumer.State]](state.pure[IO])
        consumer = TestConsumer(ref)
        headCache <- headCacheOf(consumer.pure[IO])
        result <- Concurrent[IO].start {
          headCache(key = key, partition = partition, offset = marker)
        }
        _ <- ref.update { state =>
          for {
            state <- state
          } yield {
            state.copy(topics = Map((topic, List(partition))))
          }
        }
        _ <- ref.update { state =>
          for {
            state <- state
          } yield {
            val action = Action.Mark(key, timestamp, none, "mark")
            val record = ConsumerRecordOf(action, topicPartition, marker)
            val records = ConsumerRecordsOf(List(record))
            state.copy(records = state.records.enqueue(records))
          }
        }
        result <- result.join
        state <- ref.get
        state <- state
      } yield {
        state shouldEqual TestConsumer.State(
          assigns = List(TestConsumer.Assign(topic, Nel(0))),
          seeks = List(TestConsumer.Seek(topic, Map((partition, 0)))),
          topics = Map((topic, List(partition))))

        result shouldEqual Some(HeadCache.Result(seqNr = none, deleteTo = none))
      }

      result.run()
    }

    "clean cache after events are being replicated" ignore {
      val key = Key(id = "id", topic = topic)

      val offsetLast = 10l
      val records = for {
        offset <- (0l until offsetLast).toList
        seqNr <- SeqNr.opt(offset + 1)
      } yield {
        val action = Action.Append(key, timestamp, none, Nel(Event(seqNr)))
        val record = ConsumerRecordOf(action, topicPartition, offset)
        ConsumerRecordsOf(List(record))
      }

      val state = TestConsumer.State(
        topics = Map((topic, List(0))),
        records = Queue(records: _*))

      val result = for {
        pointers <- Ref.of[IO, Map[Partition, Offset]](Map.empty)
        consumerState <- Ref.of[IO, IO[TestConsumer.State]](state.pure[IO])
        consumer = TestConsumer(consumerState).pure[IO]
        headCache <- {
          val topicPointers = for {
            pointers <- pointers.get
          } yield TopicPointers(pointers)
          implicit val eventual = HeadCache.Eventual.const(topicPointers)
          headCacheOf(consumer)
        }
        result <- headCache(
          key = key,
          partition = partition,
          offset = offsetLast)
        _ <- pointers.update { pointers => pointers ++ Map((partition, offsetLast)) }
      } yield {
        state shouldEqual TestConsumer.State(
          assigns = List(TestConsumer.Assign(topic, Nel(0))),
          seeks = List(TestConsumer.Seek(topic, Map((partition, 0)))),
          topics = Map((topic, List(partition))))

        result shouldEqual Some(HeadCache.Result(seqNr = none, deleteTo = none))
      }

      result.run()
    }

    "invalidate cache in case exceeding maxSize" in {
      val state = TestConsumer.State(
        topics = Map((topic, List(0))))

      val config = HeadCacheSpec.config.copy(maxSize = 1)

      val result = for {
        pointers <- Ref.of[IO, Map[Partition, Offset]](Map.empty)
        ref <- Ref.of[IO, IO[TestConsumer.State]](state.pure[IO])
        consumer = TestConsumer(ref)
        headCache <- {
          val topicPointers = for {
            pointers <- pointers.get
          } yield TopicPointers(pointers)
          implicit val eventual = HeadCache.Eventual.const(topicPointers)
          headCacheOf(consumer.pure[IO], config)
        }
        key0 = Key(id = "id0", topic = topic)
        key1 = Key(id = "id1", topic = topic)
        enqueue = (key: Key, offset: Offset) => {
          ref.update { state =>
            for {
              state <- state
            } yield {
              val action = Action.Append(key, timestamp, none, Nel(Event(SeqNr.Min)))
              val record = ConsumerRecordOf(action, topicPartition, offset)
              val records = ConsumerRecordsOf(List(record))
              state.copy(records = state.records.enqueue(records))
            }
          }
        }
        _ <- enqueue(key0, 0l)
        r0 <- headCache(key0, partition, 0l)
        _ <- enqueue(key1, 1l)
        r1 <- headCache(key0, partition, 1l)
        r2 <- headCache(key1, partition, 1l)
        _ <- pointers.update(_ ++ Map((partition, 1l)))
        r3 <- headCache(key1, partition, 1l)
        _ <- enqueue(key0, 2l)
        r4 <- headCache(key0, partition, 2l)
        state <- ref.get
        state <- state
      } yield {
        state shouldEqual TestConsumer.State(
          assigns = List(TestConsumer.Assign(topic, Nel(0))),
          seeks = List(TestConsumer.Seek(topic, Map((partition, 0)))),
          topics = Map((topic, List(partition))))
        r0 shouldEqual Some(HeadCache.Result(seqNr = Some(SeqNr.Min), deleteTo = none))
        r1 shouldEqual none
        r2 shouldEqual none
        r3 shouldEqual none
        r4 shouldEqual Some(HeadCache.Result(seqNr = Some(SeqNr.Min), deleteTo = none))
      }

      result.run()
    }
  }
}

object HeadCacheSpec {
  val timestamp: Instant = Instant.now()
  val topic: Topic = "topic"
  val partition: Partition = 0
  val topicPartition: TopicPartition = TopicPartition(topic = topic, partition = partition)
  val config: HeadCache.Config = HeadCache.Config(
    pollTimeout = 3.millis,
    cleanInterval = 100.millis)

  def headCacheOf(
    consumer: IO[HeadCache.Consumer[IO]],
    config: HeadCache.Config = config)(implicit
    eventual: HeadCache.Eventual[IO]): IO[HeadCache[IO]] = {

    HeadCache.of[IO](
      config = config,
      consumer = Resource.liftF(consumer))
  }

  implicit val log: Log[IO] = Log.empty[IO]

  object TestConsumer {

    def apply(ref: Ref[IO, IO[State]])(implicit timer: Timer[IO]): HeadCache.Consumer[IO] = {
      new HeadCache.Consumer[IO] {

        def assign(topic: Topic, partitions: Nel[Partition]) = {
          ref.update { state =>
            for {
              state <- state
            } yield {
              state.copy(assigns = Assign(topic, partitions) :: state.assigns)
            }
          }
        }

        def seek(topic: Topic, offsets: Map[Partition, Offset]) = {
          ref.update { state =>
            for {
              state <- state
            } yield {
              state.copy(seeks = Seek(topic, offsets) :: state.seeks)
            }
          }
        }

        def poll(timeout: FiniteDuration) = {
          for {
            _ <- timer.sleep(timeout)
            records <- ref.modify { state =>
              val result = for {
                state <- state
              } yield {
                state.records.dequeueOption match {
                  case None                    => (state, ConsumerRecords.empty[Id, Bytes])
                  case Some((record, records)) =>
                    val stateUpdated = state.copy(records = records)
                    (stateUpdated, record)
                }
              }

              // TODO use unzip like combinator
              (result.map(_._1), result.map(_._2))
            }
            records <- records
          } yield {
            records
          }
        }

        def partitions(topic: Topic) = {
          for {
            state <- ref.get
            state <- state
          } yield {
            state.topics.getOrElse(topic, Nil)
          }
        }
      }
    }


    final case class Assign(topic: Topic, partitions: Nel[Partition])

    final case class Seek(topic: Topic, offsets: Map[Partition, Offset])

    final case class State(
      assigns: List[Assign] = List.empty,
      seeks: List[Seek] = List.empty,
      topics: Map[Topic, List[Partition]] = Map.empty,
      records: Queue[ConsumerRecords[Id, Bytes]] = Queue.empty)

    object State {
      val Empty: State = State()
    }
  }
}