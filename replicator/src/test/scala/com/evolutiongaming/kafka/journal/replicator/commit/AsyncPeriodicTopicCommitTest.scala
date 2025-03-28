package com.evolutiongaming.kafka.journal.replicator.commit

import cats.data.{NonEmptyMap, NonEmptySet}
import cats.effect.{Ref, SyncIO}
import com.evolutiongaming.catshelper.{Log, ToTry}
import com.evolutiongaming.kafka.journal.KafkaConsumer
import com.evolutiongaming.kafka.journal.replicator.EmptyRebalanceConsumer
import com.evolutiongaming.kafka.journal.replicator.SKafkaTestUtils.*
import com.evolutiongaming.skafka.*
import com.evolutiongaming.skafka.consumer.{ConsumerRecords, RebalanceCallback, RebalanceConsumer, RebalanceListener1}
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers

import scala.collection.mutable
import scala.concurrent.duration.*
import scala.util.Try

class AsyncPeriodicTopicCommitTest extends AnyFreeSpec with Matchers {
  import AsyncPeriodicTopicCommitTest.*

  "AsyncPeriodicTopicCommit" - {
    "should perform periodic async commit for assigned partitions" in {
      val consumer = new TestConsumer
      val impl     = makeImpl(consumer)
      consumer.runCbUnsafe(impl.onPartitionsAssigned(NonEmptySet.of(p(0), p(1))))

      impl(
        NonEmptyMap.of(
          p(0) -> offset(1),
          p(1) -> offset(11),
          p(2) -> offset(21),
        ),
      ).unsafeRunSync()
      consumer.getRecordedActions shouldEqual Vector()

      impl.periodicCommitStep().unsafeRunSync()
      consumer.getRecordedActions shouldEqual Vector(
        Action.AsyncCommit(
          NonEmptyMap.of(
            tp(topic, 0) -> OffsetAndMetadata(offset(1), commitMetadata),
            tp(topic, 1) -> OffsetAndMetadata(offset(11), commitMetadata),
          ),
        ),
      )

      consumer.clearRecordedActions()
      impl(
        NonEmptyMap.of(
          p(0) -> offset(2),
        ),
      ).unsafeRunSync()
      consumer.getRecordedActions shouldEqual Vector()

      impl.periodicCommitStep().unsafeRunSync()
      consumer.getRecordedActions shouldEqual Vector(
        Action.AsyncCommit(
          NonEmptyMap.of(
            tp(topic, 0) -> OffsetAndMetadata(offset(2), commitMetadata),
          ),
        ),
      )
    }

    "should perform sync commit for revoked partitions" in {
      val consumer = new TestConsumer
      val impl     = makeImpl(consumer)
      consumer.runCbUnsafe(impl.onPartitionsAssigned(NonEmptySet.of(p(0), p(1), p(2))))

      impl(
        NonEmptyMap.of(
          p(0) -> offset(1),
          p(1) -> offset(11),
          p(2) -> offset(21),
        ),
      ).unsafeRunSync()

      consumer.getRecordedActions shouldEqual Vector()

      consumer.runCbUnsafe(impl.onPartitionsRevoked(NonEmptySet.of(p(0), p(2))))

      consumer.getRecordedActions shouldEqual Vector(
        Action.SyncCommit(
          NonEmptyMap.of(
            tp(topic, 0) -> OffsetAndMetadata(offset(1), commitMetadata),
            tp(topic, 2) -> OffsetAndMetadata(offset(21), commitMetadata),
          ),
        ),
      )
    }

    "should ignore lost partitions" in {
      val consumer = new TestConsumer
      val impl     = makeImpl(consumer)
      consumer.runCbUnsafe(impl.onPartitionsAssigned(NonEmptySet.of(p(0), p(1), p(2))))

      impl(
        NonEmptyMap.of(
          p(0) -> offset(1),
          p(1) -> offset(11),
          p(2) -> offset(21),
        ),
      ).unsafeRunSync()

      consumer.getRecordedActions shouldEqual Vector()

      consumer.runCbUnsafe(impl.onPartitionsLost(NonEmptySet.of(p(0), p(2))))

      consumer.getRecordedActions shouldEqual Vector()

      impl.periodicCommitStep().unsafeRunSync()

      consumer.getRecordedActions shouldEqual Vector(
        Action.AsyncCommit(
          NonEmptyMap.of(
            tp(topic, 1) -> OffsetAndMetadata(offset(11), commitMetadata),
          ),
        ),
      )
    }
  }

  private def makeImpl(consumer: TestConsumer): AsyncPeriodicTopicCommit[SyncIO] = {
    new AsyncPeriodicTopicCommit[SyncIO](
      topic          = topic,
      commitMetadata = commitMetadata,
      consumer       = consumer,
      log            = Log.empty,
      stateRef       = Ref.unsafe[SyncIO, AsyncPeriodicTopicCommit.State](AsyncPeriodicTopicCommit.State()),
    )
  }
}

private object AsyncPeriodicTopicCommitTest {
  val topic          = "my-topic"
  val commitMetadata = "my-commit-meta"

  val doNotCallF: SyncIO[Nothing] = SyncIO.raiseError(new RuntimeException("method not supposed to be called"))

  implicit val syncIoToTry: ToTry[SyncIO] = new ToTry[SyncIO] {
    override def apply[A](fa: SyncIO[A]): Try[A] = Try(fa.unsafeRunSync())
  }

  sealed trait Action
  object Action {
    final case class SyncCommit(offsets: NonEmptyMap[TopicPartition, OffsetAndMetadata]) extends Action
    final case class AsyncCommit(offsets: NonEmptyMap[TopicPartition, OffsetAndMetadata]) extends Action
  }

  private class TestConsumer extends KafkaConsumer[SyncIO, String, String] {

    private val recordedActions: mutable.ArrayBuffer[Action] = mutable.ArrayBuffer.empty[Action]

    private val rebalanceConsumer: RebalanceConsumer = new EmptyRebalanceConsumer {
      override def commit(offsets: NonEmptyMap[TopicPartition, OffsetAndMetadata]): Try[Unit] = {
        Try {
          recordedActions += Action.SyncCommit(offsets)
        }
      }
    }

    def getRecordedActions: Vector[Action] = recordedActions.toVector

    def clearRecordedActions(): Unit = recordedActions.clear()

    def runCbUnsafe(rebalanceCallback: RebalanceCallback[SyncIO, Unit]): Unit = {
      rebalanceCallback.run(rebalanceConsumer).get
    }

    override def commitLater(offsets: NonEmptyMap[TopicPartition, OffsetAndMetadata]): SyncIO[Unit] = {
      SyncIO.delay {
        recordedActions += Action.AsyncCommit(offsets)
      }
    }

    override def commit(offsets: NonEmptyMap[TopicPartition, OffsetAndMetadata]): SyncIO[Unit] = {
      SyncIO.delay {
        recordedActions += Action.SyncCommit(offsets)
      }
    }

    override def assign(partitions: NonEmptySet[TopicPartition]): SyncIO[Unit] = doNotCallF

    override def seek(partition: TopicPartition, offset: Offset): SyncIO[Unit] = doNotCallF

    override def subscribe(topic: Topic, listener: RebalanceListener1[SyncIO]): SyncIO[Unit] = doNotCallF

    override def poll(timeout: FiniteDuration): SyncIO[ConsumerRecords[String, String]] = doNotCallF

    override def topics: SyncIO[Set[Topic]] = doNotCallF

    override def partitions(topic: Topic): SyncIO[Set[Partition]] = doNotCallF

    override def assignment: SyncIO[Set[TopicPartition]] = doNotCallF
  }
}
