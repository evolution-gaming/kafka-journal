package com.evolution.kafka.journal

import cats.data.NonEmptySet as Nes
import cats.effect.*
import cats.syntax.all.*
import com.evolution.kafka.journal.HeadCacheSpec.*
import com.evolution.kafka.journal.IOSuite.*
import com.evolution.kafka.journal.TestJsonCodec.instance
import com.evolution.kafka.journal.eventual.TopicPointers
import com.evolutiongaming.catshelper.{Log, LogOf}
import com.evolutiongaming.skafka.*
import com.evolutiongaming.skafka.consumer.ConsumerRecords
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec

import scala.collection.immutable.Queue
import scala.concurrent.duration.*

class ShardedHeadCacheSpec extends AsyncWordSpec with Matchers {
  import ShardedHeadCacheSpec.*

  "ShardedHeadCache" should {
    "start eager topics in their consumer groups and seek assigned partitions to the replicated offsets" in {
      val pointers = TopicPointers(Map((partition0, Offset.unsafe(10))))
      val eventual = HeadCache.Eventual.const(pointers.pure[IO])
      val result = for {
        transport <- TestTransport.make
        pod <- TestPod.make
        _ <- headCacheOf(eventual, pod, transport, eagerTopics = Set(topic)).use { _ =>
          for {
            offsets <- pod.assign(Nes.of(partition0, partition1))
            state <- pod.state.get
          } yield {
            offsets shouldEqual Map((partition0, Offset.unsafe(11).some), (partition1, none))
            state.groupIds shouldEqual Vector(s"app-headcache-$topic")
            state.subscriptions shouldEqual Vector(topic)
          }
        }
      } yield ()
      result.run()
    }

    "start other topics on the first request" in {
      val key = Key(id = "id", topic = topic)
      val result = for {
        transport <- TestTransport.make
        pod <- TestPod.make
        _ <- headCacheOf(HeadCache.Eventual.empty[IO], pod, transport, eagerTopics = Set.empty).use { headCache =>
          for {
            before <- pod.state.get
            _ = before.groupIds shouldEqual Vector.empty
            result <- headCache.get(key, partition0, Offset.unsafe(10))
            _ = result shouldEqual none
            _ <- pod.listener
            after <- pod.state.get
          } yield {
            after.groupIds shouldEqual Vector(s"app-headcache-$topic")
          }
        }
      } yield ()
      result.run()
    }

    "answer from the local cache for an assigned partition, without the transport" in {
      val key = Key(id = "id", topic = topic)
      val result = for {
        transport <- TestTransport.make
        pod <- TestPod.make
        _ <- headCacheOf(HeadCache.Eventual.empty[IO], pod, transport).use { headCache =>
          for {
            _ <- pod.assign(Nes.of(partition0))
            _ <- pod.enqueue(appends(key, partition0, Offset.unsafe(10)))
            result <- headCache.get(key, partition0, Offset.unsafe(10))
            requests <- transport.requests.get
          } yield {
            result shouldEqual HeadInfo.append(Offset.min, SeqNr.unsafe(11), none).some
            requests shouldEqual 0
          }
        }
      } yield ()
      result.run()
    }

    "ask the owner of a partition assigned to another pod" in {
      val key = Key(id = "id", topic = topic)
      val result = for {
        transport <- TestTransport.make
        owner <- TestPod.make
        other <- TestPod.make
        _ <- (
          headCacheOf(HeadCache.Eventual.empty[IO], owner, transport),
          headCacheOf(HeadCache.Eventual.empty[IO], other, transport),
        ).tupled.use {
          case (_, otherCache) =>
            for {
              _ <- owner.assign(Nes.of(partition0))
              _ <- other.assign(Nes.of(partition1))
              _ <- owner.enqueue(appends(key, partition0, Offset.unsafe(10)))
              result <- otherCache.get(key, partition0, Offset.unsafe(10))
              requests <- transport.requests.get
            } yield {
              result shouldEqual HeadInfo.append(Offset.min, SeqNr.unsafe(11), none).some
              requests shouldEqual 1
            }
        }
      } yield ()
      result.run()
    }

    "stop serving a revoked partition and give no answer to requests waiting on it" in {
      val key = Key(id = "id", topic = topic)
      val result = for {
        transport <- TestTransport.make
        pod <- TestPod.make
        _ <- headCacheOf(HeadCache.Eventual.empty[IO], pod, transport).use { headCache =>
          for {
            _ <- pod.assign(Nes.of(partition0))
            served <- transport.served
            _ = served shouldEqual Set((topic, partition0))
            // the consumer has not reached this offset, so the request waits
            waiting <- headCache.get(key, partition0, Offset.unsafe(100)).start
            _ <- IO.sleep(50.millis)
            _ <- pod.revoke(Nes.of(partition0))
            waiting <- waiting.joinWithNever
            served <- transport.served
            afterRevoke <- headCache.get(key, partition0, Offset.unsafe(100))
          } yield {
            waiting shouldEqual none
            served shouldEqual Set.empty
            afterRevoke shouldEqual none
          }
        }
      } yield ()
      result.run()
    }

    "ignore records of partitions that are not assigned" in {
      val key = Key(id = "id", topic = topic)
      val result = for {
        transport <- TestTransport.make
        pod <- TestPod.make
        _ <- headCacheOf(HeadCache.Eventual.empty[IO], pod, transport).use { headCache =>
          for {
            _ <- pod.assign(Nes.of(partition0))
            _ <- pod.enqueue(appends(key, partition1, Offset.unsafe(10)))
            _ <- pod.enqueue(appends(key, partition0, Offset.unsafe(10)))
            result <- headCache.get(key, partition0, Offset.unsafe(10))
            state <- pod.state.get
          } yield {
            result shouldEqual HeadInfo.append(Offset.min, SeqNr.unsafe(11), none).some
            // the consumer did not fail and was not created again
            state.groupIds.size shouldEqual 1
          }
        }
      } yield ()
      result.run()
    }

    "release partitions when the consumer is closed" in {
      val result = for {
        transport <- TestTransport.make
        pod <- TestPod.make
        _ <- headCacheOf(HeadCache.Eventual.empty[IO], pod, transport).use { _ =>
          pod.assign(Nes.of(partition0, partition1))
        }
        served <- transport.served
      } yield {
        served shouldEqual Set.empty
      }
      result.run()
    }
  }
}

object ShardedHeadCacheSpec {
  val partition0: Partition = Partition.min
  val partition1: Partition = Partition.unsafe(1)

  implicit val log: Log[IO] = Log.empty[IO]
  implicit val logOf: LogOf[IO] = LogOf.empty[IO]

  def headCacheOf(
    eventual: HeadCache.Eventual[IO],
    pod: TestPod,
    transport: HeadCacheTransport[IO],
    eagerTopics: Set[Topic] = Set(topic),
  ): Resource[IO, HeadCache[IO]] = {
    ShardedHeadCache.make[IO](
      eventual = eventual,
      consumer = (groupId: String) => pod.consumer(groupId),
      transport = transport,
      config = ShardedHeadCacheConfig(applicationId = "app", eagerTopics = eagerTopics),
      headCacheConfig = HeadCacheConfig(timeout = 500.millis, removeInterval = 100.millis),
      metrics = none,
    )
  }

  def appends(key: Key, partition: Partition, offsetLast: Offset): ConsumerRecords[String, Unit] = {
    val topicPartition = TopicPartition(topic = key.topic, partition = partition)
    val records = for {
      idx <- (0L to offsetLast.value).toVector
      seqNr <- SeqNr.opt(idx + 1)
    } yield {
      consumerRecordOf(appendOf(key, seqNr), topicPartition, Offset.unsafe(idx))
    }
    ConsumerRecordsOf(records.toList)
  }

  /**
   * A pod's consumer in a consumer group; the test triggers rebalances.
   */
  final case class TestPod(state: Ref[IO, TestPod.State]) {
    def consumer(groupId: String): Resource[IO, ShardedTopicCache.Consumer[IO]] = {
      val consumer = new TestPod.TestConsumer(state)
      Resource.make {
        state.update { state => state.copy(groupIds = state.groupIds :+ groupId) }.as(consumer)
      } { _ =>
        state.update { _.copy(listener = none) }
      }
    }

    def listener: IO[ShardedTopicCache.Listener[IO]] =
      state.get.flatMap { state =>
        state.listener.fold(IO.sleep(1.milli).as(none[ShardedTopicCache.Listener[IO]]))(_.some.pure[IO])
      }.untilDefinedM.timeout(5.seconds)

    def assign(partitions: Nes[Partition]): IO[Map[Partition, Option[Offset]]] =
      listener.flatMap(_.onAssigned(partitions))

    def revoke(partitions: Nes[Partition]): IO[Unit] =
      listener.flatMap(_.onRevoked(partitions))

    def enqueue(records: ConsumerRecords[String, Unit]): IO[Unit] =
      state.update { state => state.copy(records = state.records.enqueue(records)) }
  }

  object TestPod {
    private final class TestConsumer(state: Ref[IO, State]) extends ShardedTopicCache.Consumer[IO] {
      def subscribe(topic: Topic, listener: ShardedTopicCache.Listener[IO]): IO[Unit] = {
        state.update { state =>
          state.copy(listener = listener.some, subscriptions = state.subscriptions :+ topic)
        }
      }

      val poll: IO[ConsumerRecords[String, Unit]] = {
        IO.sleep(1.milli) *> state.modify { state =>
          state.records.dequeueOption match {
            case Some((records, queue)) => (state.copy(records = queue), records)
            case None => (state, ConsumerRecords.empty[String, Unit])
          }
        }
      }
    }

    final case class State(
      groupIds: Vector[String] = Vector.empty,
      subscriptions: Vector[Topic] = Vector.empty,
      listener: Option[ShardedTopicCache.Listener[IO]] = none,
      records: Queue[ConsumerRecords[String, Unit]] = Queue.empty,
    )

    def make: IO[TestPod] = Ref[IO].of(State()).map(TestPod.apply)
  }

  final case class TestTransport(
    handlers: Ref[IO, Map[(Topic, Partition), HeadCacheRequest => IO[Option[HeadInfo]]]],
    requests: Ref[IO, Int],
  ) extends HeadCacheTransport[IO] {
    def served: IO[Set[(Topic, Partition)]] = handlers.get.map(_.keySet)

    def serve(
      topic: Topic,
      partition: Partition,
      handle: HeadCacheRequest => IO[Option[HeadInfo]],
    ): Resource[IO, Unit] = Resource.make {
      handlers.update(_.updated((topic, partition), handle))
    } { _ =>
      handlers.update(_ - ((topic, partition)))
    }

    def request(request: HeadCacheRequest): IO[Option[HeadInfo]] = {
      for {
        _ <- requests.update(_ + 1)
        handlers <- handlers.get
        answer <- handlers.get((request.topic, request.partition)).flatTraverse { handle => handle(request) }
      } yield answer
    }
  }

  object TestTransport {
    def make: IO[TestTransport] = {
      for {
        handlers <- Ref[IO].of(Map.empty[(Topic, Partition), HeadCacheRequest => IO[Option[HeadInfo]]])
        requests <- Ref[IO].of(0)
      } yield TestTransport(handlers, requests)
    }
  }
}
