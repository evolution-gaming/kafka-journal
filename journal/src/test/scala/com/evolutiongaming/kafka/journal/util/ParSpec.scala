package com.evolutiongaming.kafka.journal.util

import cats.effect.concurrent.Deferred
import cats.effect.{Concurrent, IO}
import cats.implicits._
import cats.kernel.CommutativeMonoid
import com.evolutiongaming.kafka.journal.util.IOSuite._
import org.scalatest.{AsyncFunSuite, Matchers}

class ParSpec extends AsyncFunSuite with Matchers {

  test("fold") {

    implicit val commutativeApplicative: CommutativeMonoid[List[Int]] = new CommutativeMonoid[List[Int]] {
      def empty = List.empty
      def combine(x: List[Int], y: List[Int]) = x ++ y
    }

    val result = for {
      deferred0 <- Deferred[IO, List[Int]]
      deferred1 <- Deferred[IO, Unit]
      result    <- Concurrent[IO].start {
        val v0 = deferred0.get
        val v1 = deferred1.complete(()).map { _ => List(1) }
        Par[IO].fold(List(v0, v1))
      }
      _       <- deferred1.get
      _       <- deferred0.complete(List(0))
      result  <- result.join
    } yield {
      result shouldEqual List(0, 1)
    }

    result.run()
  }
}
