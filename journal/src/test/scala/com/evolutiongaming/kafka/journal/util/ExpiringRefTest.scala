package com.evolutiongaming.kafka.journal.util

import cats.syntax.all._
import cats.effect.{Deferred, IO}
import com.evolutiongaming.catshelper.Log
import com.evolutiongaming.kafka.journal.IOSuite._
import org.scalatest.funsuite.AsyncFunSuite
import org.scalatest.matchers.should.Matchers

import scala.concurrent.duration._

class ExpiringRefTest extends AsyncFunSuite with Matchers {

  implicit val log = Log.empty[IO]

  test("ExpireRef.of is non-blocking") {
    val result = for {
      l <- Deferred[IO, Unit]
      _ <- ExpiringRef
        .of[IO, Unit](l.get, 1.minute)
        .use { _ =>
          IO { succeed }
        }
    } yield {}
    result.run(1.second)
  }

  test("ExpireRef#get is semantically blocking") {
    val result = for {
      l <- Deferred[IO, Unit]
      d <- Deferred[IO, Unit]
      f <- ExpiringRef
        .of[IO, Unit](l.get, 1.minute)
        .use { ref => ref.get >> d.complete({}) }
        .start
      _ <- d.tryGet.map { opt => opt shouldBe none[Unit] }
      _ <- l.complete({})
      _ <- f.join
    } yield {}
    result.run(1.second)
  }

  test("reload value after expiration") {
    val result = for {
      v <- IO.ref(0)
      l = IO.sleep(10.millis) >> v.get
      _ <- ExpiringRef
        .of[IO, Int](l, 100.millis)
        .use { ref =>
          for {
            v0 <- ref.get
            _ <- IO { v0 shouldBe 0 }
            _ <- v.set(1)
            _ <- IO.sleep(100.millis)
            _ <- ref.get.map { v1 => v1 shouldBe 1 }.eventually
          } yield {}
        }
    } yield {}
    result.run(1.second)
  }

  test("raise exception if reload constantly fails") {
    val result = for {
      v <- IO.ref(none[Throwable])
      l = IO.sleep(10.millis) >> v.get.flatMap(_.traverse(_.raiseError[IO, Unit]).void)
      _ <- ExpiringRef
        .of[IO, Unit](l, 100.millis)
        .use { ref =>
          val err = new RuntimeException("test exception")
          for {
            _ <- ref.get
            _ <- v.set(err.some)
            _ <- IO.sleep(100.millis)
            _ <- ref.get.attempt.map { e => e shouldBe err.asLeft[Unit] }.eventually
          } yield {}
        }
    } yield {}
    result.run(1.second)
  }


}
