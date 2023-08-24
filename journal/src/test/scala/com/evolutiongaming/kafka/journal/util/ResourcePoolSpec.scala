package com.evolutiongaming.kafka.journal.util

import cats.effect.{IO, Ref}
import cats.effect.kernel.Resource

import scala.concurrent.duration._
import com.evolutiongaming.catshelper.LogOf
import cats.syntax.all._
import cats.effect.syntax.all._
import cats.effect.unsafe.implicits.global
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.util.UUID

class ResourcePoolSpec extends AnyFunSuite with Matchers {

  case class TestObject(id: UUID) {
    override def toString = id.toString.take(8)
  }

  object TestObject {
    def create = IO.delay(UUID.randomUUID()).map(TestObject(_))
  }

  implicit val logOf: LogOf[IO] = LogOf.slf4j[IO].unsafeRunSync()

  test("Pool should allocate and deallocate exactly `poolSize` objects") {
    // TODO: use CE3 test runtime after dropping CE2 support.
    // It will allow increasing sleepDuration but no actual sleeping will happen.
    val poolSize = 16
    val sleepDuration = 20.millis
    val timesToAcquire = 1000
    val resourceTTL = 1.second
    val acquireTimeout = 30.seconds

    (for {
      allocateCount <- Ref.of[IO, Int](0)
      releaseCount <- Ref.of[IO, Int](0)
      log <- LogOf.log[IO](getClass.getName)
      objectOf = Resource.make {
        allocateCount.update(_ + 1) *> TestObject.create
      } { _ =>
        releaseCount.update(_ + 1)
      }
      _ <- ResourcePool.fixedSize[IO, TestObject](
        objectOf,
        poolSize,
        acquireTimeout = acquireTimeout,
        resourceTTL = resourceTTL,
        log = log,
      ).use { pool =>
        for {
          _ <- (1 to timesToAcquire).toVector.parTraverse_ { _ =>
            pool.borrow.use(_ => IO.sleep(sleepDuration)) // emulate using an object
          }

          _ <- IO.sleep(resourceTTL + 100.millis) // all objects in the pool should get deallocated

          _ <- (1 to timesToAcquire).toVector.parTraverse_ { _ =>
            pool.borrow.use(_ => IO.sleep(sleepDuration))
          }
        } yield ()
      }
      _ <- allocateCount.get.map(_ shouldEqual poolSize * 2)
      _ <- releaseCount.get.map(_ shouldEqual poolSize * 2)
    } yield ()).unsafeRunSync()
  }
}
