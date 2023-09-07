package com.evolutiongaming.kafka.journal.util

import cats.effect.{IO, Ref}
import cats.effect.kernel.Resource
import cats.effect.syntax.all._

import scala.concurrent.duration._
import com.evolutiongaming.catshelper.{Log, LogOf}
import cats.syntax.all._
import cats.effect.unsafe.implicits.global
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.util.UUID
import cats.effect.kernel.Deferred

class ObjectPoolSpec extends AnyFunSuite with Matchers {
  // TODO: use CE3 test runtime after dropping CE2 support.
  // With CE3 test runtime no actual sleeping will happen and tests will be much faster.

  case class TestObject(id: UUID) {
    override def toString = id.toString.take(8)
  }

  object TestObject {
    def create = IO.delay(UUID.randomUUID()).map(TestObject(_))
  }

  def await(step: FiniteDuration, timeout: FiniteDuration)(condition: IO[Boolean]): IO[Unit] =
    false.iterateUntilM(_ => IO.sleep(step) *> condition)(_ == true).timeout(timeout).void

  case class Fixture(
    pool: ObjectPool[IO, TestObject],
    allocateCount: Ref[IO, Int],
    deallocateCount: Ref[IO, Int],
    log: Log[IO],
  )

  object Fixture {

    implicit val logOf: LogOf[IO] = LogOf.slf4j[IO].unsafeRunSync()

    def make(
      poolSize: Int = 16,
      idleTimeout: FiniteDuration = 30.seconds,
      customAllocate: Option[IO[TestObject]] = None,
      customDeallocate: Option[TestObject => IO[Unit]] = None,
    ): Resource[IO, Fixture] = {
      for {
        log <- LogOf.log[IO](getClass.getName).toResource

        allocateCount <- Ref.of[IO, Int](0).toResource
        deallocateCount <- Ref.of[IO, Int](0).toResource

        makeObject = Resource.make {
          customAllocate match {
            case Some(alloc) => allocateCount.update(_ + 1) *> alloc
            case None => allocateCount.update(_ + 1) *> TestObject.create
          }
        } {
          customDeallocate match {
            case Some(dealloc) => { obj =>
              deallocateCount.update(_ + 1) *> dealloc(obj)
            }
            case None => { _ =>
              deallocateCount.update(_ + 1)
            }
          }
        }

        pool <- ObjectPool.fixedSize[IO, TestObject](
          poolSize,
          idleTimeout = idleTimeout,
          log = log,
        )(makeObject)
      } yield Fixture(
        pool = pool,
        allocateCount = allocateCount,
        deallocateCount = deallocateCount,
        log = log,
      )
    }
  }

  /**
   * We use a latch to ensure that `poolSize` objects are used at a given moment in time.
   * Without a latch it's possible that fewer than `poolSize` objects will be allocated
   *
   * Probably [[cats.effect.std.CountDownLatch]] can be used instead of this after we drop CE2 support.
   */
  trait Latch {
    def await: IO[Unit]
  }

  object Latch {

    def apply(count: Int): IO[Latch] = {
      for {
        unblock <- Deferred[IO, Unit]
        countRef <- Ref.of[IO, Int](count)
      } yield new Latch {
        override def await: IO[Unit] = {
          countRef.updateAndGet(_ - 1).flatMap { count =>
            if (count <= 0) {
              unblock.complete(()).void
            } else {
              unblock.get
            }
          }
        }
      }
    }
  }

  test("Max number of objects should be limited") {
    val poolSize = 16
    val timesToAcquire = 100

    Fixture.make(
      poolSize = poolSize
    ).use { fixture =>
      import fixture._

      for {
        latch <- Latch(poolSize)
        _ <- (1 to timesToAcquire).toVector.parTraverse_ { _ =>
          pool.borrow.use(_ => latch.await)
        }

        _ <- allocateCount.get.map(_ shouldEqual poolSize)
      } yield ()
    }.unsafeRunSync()
  }

  test("Pool should deallocate objects if they're not used for `idleTimeout` time") {
    val poolSize = 16
    val timesToAcquire = 100

    Fixture.make(
      poolSize = poolSize,
      idleTimeout = 100.millis,
    ).use { fixture =>
      import fixture._

      for {
        latch <- Latch(poolSize)
        _ <- (1 to timesToAcquire).toVector.parTraverse_ { _ =>
          pool.borrow.use(_ => latch.await)
        }

        // All objects in the pool should get deallocated because they're idle.
        _ <- await(100.millis, 30.seconds) {
          deallocateCount.get.map(_ == poolSize)
        }
      } yield ()

    }.unsafeRunSync()
  }

  test("Pool should be able to do multiple rounds of allocation/deallocation") {
    val poolSize = 4
    val timesToAcquire = 50

    Fixture.make(
      poolSize = poolSize,
      idleTimeout = 100.millis,
    ).use { fixture =>
      import fixture._

      for {
        // Trigger allocation of objects
        latch <- Latch(poolSize)
        _ <- (1 to timesToAcquire).toVector.parTraverse_ { _ =>
          pool.borrow.use(_ => latch.await)
        }

        // All objects in the pool should get deallocated because they're idle.
        _ <- await(100.millis, 30.seconds) {
          deallocateCount.get.map(_ == poolSize)
        }

        // Trigger a 2nd round of allocations
        latch <- Latch(poolSize)
        _ <- (1 to timesToAcquire).toVector.parTraverse_ { _ =>
          pool.borrow.use(_ => latch.await)
        }

        // Check that there were 2 rounds of allocations.
        _ <- allocateCount.get.map(_ shouldEqual poolSize * 2)

        // Wait for the second round of deallocations
        _ <- await(100.millis, 30.seconds) {
          deallocateCount.get.map(_ == 2 * poolSize)
        }
      } yield ()
    }.unsafeRunSync()
  }

  test("Object should be returned to the pool even if user cancels the fiber while using the object") {
    Fixture.make(
      poolSize = 1
    ).use { fixture =>
      import fixture._

      for {
        d <- Deferred[IO, Unit]
        userFiber <- pool.borrow.use(_ => d.complete(()) *> IO.never).start
        _ <- d.get

        // Now `userFiber` is in the `use` section of the code.
        // Let's cancel it and wait for the object to be returned to the pool.
        _ <- userFiber.cancel

        // Previously created object should be reused.
        _ <- pool.borrow.use(_ => IO.unit)
        _ <- allocateCount.get.map(_ shouldEqual 1)
      } yield ()
    }.unsafeRunSync()
  }

  test("Object should be returned to the pool even if user cancels the fiber while acquiring the object") {
    Deferred[IO, Unit].flatMap { unblockAllocation =>
      Fixture.make(
        poolSize = 1,
        customAllocate = Some(unblockAllocation.get *> TestObject.create)
      ).use { fixture =>
        import fixture._

        for {
          userFiber <- pool.borrow.use(_ => IO.never).start

          // `userFiber` is stuck in the `acquire` section of code (because we blocked allocation).
          // Our pool should support fiber cancellation in this situation.
          cancellationFiber <- userFiber.cancel.start

          // unblock the whole acquire -> release cycle, so that `cancellationFiber` can complete
          _ <- unblockAllocation.complete(())

          // Wait for the object to be returned back to the pool.
          _ <- cancellationFiber.join

          // Previously created object should be reused.
          _ <- pool.borrow.use(_ => IO.unit)
          _ <- allocateCount.get.map(_ shouldEqual 1)
        } yield ()
      }
    }.unsafeRunSync()
  }
}
