package com.evolutiongaming.kafka.journal.util

import cats.effect.IO
import cats.implicits._
import com.evolutiongaming.kafka.journal.util.IOSuite._
import org.scalatest.{AsyncFunSuite, Matchers}

import scala.concurrent.duration._


class CacheSpec extends AsyncFunSuite with Matchers {

  test("getOrUpdate") {

    val result = for {
      cache <- Cache.of[IO, Int, Int]
      update0 = cache.getOrUpdate(0, timer.sleep(10.millis) *> 0.pure[IO])
      update1 = timer.sleep(10.millis) *> cache.getOrUpdate(0, 1.pure[IO])
      results <- List(update0, update1).parSequence
    } yield {
      results shouldEqual List(0, 0)
    }

    result.run
  }
}
