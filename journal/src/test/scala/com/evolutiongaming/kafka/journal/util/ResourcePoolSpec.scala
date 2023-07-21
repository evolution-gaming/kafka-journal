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

  implicit val logOf: LogOf[IO] = LogOf.empty

  test("Pool should allocate and deallocate exactly `poolSize` objects") {
    val poolSize = 250

    val sleepDuration = 20.milliseconds
    // TODO: use CE3 test runtime after dropping CE2 support.
    // It will allow increasing sleepDuration but no actual sleeping will happen.

    val timesToAcquire = 8000

    (for {
      useCount <- Ref.of[IO, Int](0)
      releaseCount <- Ref.of[IO, Int](0)
      log <- LogOf.log[IO](getClass.getName)
      objectOf = Resource.make {
        useCount.update(_ + 1) *> TestObject.create
      } { _ =>
        releaseCount.update(_ + 1)
      }
      _ <- ResourcePool.fixedSize[IO, TestObject](objectOf, poolSize).use { pool =>
        (1 to timesToAcquire).toVector.parTraverse_ { _ =>
          pool.borrow.use(_ => IO.sleep(sleepDuration)) // emulate using an object
        }
      }
      _ <- useCount.get.map(_ shouldEqual poolSize)
      _ <- releaseCount.get.map(_ shouldEqual poolSize)
    } yield ()).unsafeRunSync()
  }
}
