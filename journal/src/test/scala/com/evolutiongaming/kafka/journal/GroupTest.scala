package com.evolutiongaming.kafka.journal

import cats.data.{NonEmptyList => Nel}
import cats.effect.kernel.Ref
import cats.effect.{Deferred, IO, Temporal}
import cats.syntax.all._
import com.evolutiongaming.kafka.journal.IOSuite._
import org.scalatest.funsuite.AsyncFunSuite
import org.scalatest.matchers.should.Matchers

import scala.concurrent.TimeoutException
import scala.concurrent.duration._

class GroupTest extends AsyncFunSuite with Matchers {

  test("release waits until completed") {
    val result = for {
      deferred0 <- Deferred[IO, Unit]
      deferred1 <- Deferred[IO, Unit]
      deferred2 <- Deferred[IO, Nel[Unit]]
      deferred3 <- Deferred[IO, Unit]
      result <- Group.of {
        for {
          _ <- deferred0.complete(())
          _ <- deferred1.get
        } yield { (as: Nel[Unit]) =>
          for {
            _ <- deferred2.complete(as)
            a <- deferred3.get
          } yield a
        }
      }.allocated
      (group, release) = result
      _               <- group.apply(()).start
      _               <- deferred0.get
      fiber           <- release.start
      result          <- fiber.join.timeout(10.millis).attempt
      _               <- IO(result should matchPattern { case Left(_: TimeoutException) => () })
      _               <- deferred1.complete(())
      result          <- deferred2.get
      _               <- IO(result shouldEqual Nel.of(()))
      _               <- deferred3.complete(())
      _               <- fiber.join
    } yield {}
    result.run()
  }

  test("group many") {
    val result = for {
      ref <- Ref[IO].of(0).toResource
      group <- Group.of {
        for {
          a <- ref.modify(a => (a + 1, a))
          _ <- Temporal[IO].sleep(1.millis)
        } yield { (as: Nel[Int]) =>
          Temporal[IO]
            .sleep(1.millis)
            .as((a, as.combineAll))
        }
      }
    } yield {
      val x = 100
      val y = 100
      for {
        maps <- group
          .apply(1)
          .parReplicateA(x)
          .map(_.toMap)
          .parReplicateA(y)
        map    = maps.foldLeft(Map.empty[Int, Int])(_ ++ _)
        size  <- ref.get
        values = x * y
        _     <- IO(size should be < values)
        _     <- IO(map.values.sum shouldEqual values)
        _     <- IO(map.size shouldEqual size)
      } yield {}
    }
    result
      .use(identity)
      .run()
  }
}
