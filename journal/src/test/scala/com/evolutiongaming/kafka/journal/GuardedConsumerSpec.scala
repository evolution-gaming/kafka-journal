package com.evolutiongaming.kafka.journal

import cats.data.NonEmptySet
import cats.effect._
import cats.effect.std.Semaphore
import cats.syntax.all._
import com.evolutiongaming.catshelper.LogOf
import cats.effect.unsafe.implicits.global
import com.evolutiongaming.skafka.{Offset, TopicPartition}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import scala.concurrent.duration._
import java.util.UUID

class GuardedConsumerSpec extends AnyFunSuite with Matchers {

  implicit val logOf: LogOf[IO] = LogOf.empty

  case class TestConsumer(id: UUID) extends Journals.Consumer[IO] {
    override def assign(partitions: NonEmptySet[TopicPartition]): IO[Unit] = ???
    override def seek(partition: TopicPartition, offset: Offset): IO[Unit] = ???
    override def poll: IO[ConsRecords] = ???
    override def toString = id.toString.take(8)
  }

  object TestConsumer {
    def create = IO.delay(UUID.randomUUID()).map(TestConsumer(_))
  }

  case class ConsumerCount(current: Int, max: Int) {

    def inc = ConsumerCount(current + 1, math.max(max, current + 1))

    def dec = ConsumerCount(current - 1, max)
  }

  object ConsumerCount {
    val Zero = ConsumerCount(0, 0)
  }

  def makeConsumer(count: Ref[IO, ConsumerCount]) =
    Resource.make {
      count.update(_.inc) *> TestConsumer.create
    } { _ =>
      count.update(_.dec)
    }

  test("1") {

    val permits = 16
    val sleepDuration = 2.milliseconds
    val timesToAcquire = 8000
    val acquireTimeout = 100500.hours

    (for {
      log <- LogOf.log[IO](getClass.getName)

      consumerCount <- Ref.of[IO, ConsumerCount](ConsumerCount.Zero)
      consumer = makeConsumer(consumerCount)

      semaphore <- Semaphore[IO](permits)

      guardedConsumer = GuardedConsumer.of[IO](
        consumer,
        semaphore,
        acquireTimeout,
        ConsumerSemaphoreMetrics.empty,
        "topic"
      )

      _ <- (1 to timesToAcquire).toVector.parTraverse_ { _ =>
        guardedConsumer.use(_ => IO.sleep(sleepDuration)) // emulate using a consumer
      }

      _ <- consumerCount.get.map(_.max shouldEqual permits)
    } yield ()).unsafeRunSync()
  }
}
