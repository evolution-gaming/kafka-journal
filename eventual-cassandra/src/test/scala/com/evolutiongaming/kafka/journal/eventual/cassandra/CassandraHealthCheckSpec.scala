package com.evolutiongaming.kafka.journal.eventual.cassandra

import cats.effect.{IO, Resource}
import cats.implicits._
import com.evolutiongaming.catshelper.Log
import com.evolutiongaming.kafka.journal.IOSuite._
import org.scalatest.{AsyncFunSuite, Matchers}

import scala.concurrent.duration._
import scala.util.control.NoStackTrace

class CassandraHealthCheckSpec extends AsyncFunSuite with Matchers {

  test("CassandraHealthCheck") {
    val error = (new RuntimeException with NoStackTrace).raiseError[IO, Unit]
    val healthCheck = CassandraHealthCheck.of[IO](
      initial = 0.seconds,
      interval = 1.second,
      statement = Resource.pure[IO, IO[Unit]](error),
      log = Log.empty[IO])
    val result = for {
      error <- healthCheck.use { _.error.untilDefinedM }
    } yield {
      error shouldEqual error
    }
    result.run()
  }
}
