package com.evolutiongaming.kafka.journal.eventual.cassandra

import cats.effect.{IO, Resource}
import cats.implicits._
import com.evolutiongaming.kafka.journal.Log
import com.evolutiongaming.kafka.journal.util.IOSuite._
import org.scalatest.{AsyncFunSuite, Matchers}

import scala.concurrent.duration._
import scala.util.control.NoStackTrace

class CassandraHealthCheckSpec extends AsyncFunSuite with Matchers {

  test("CassandraHealthCheck") {
    implicit val log = Log.empty[IO]
    val error = (new RuntimeException with NoStackTrace).raiseError[IO, Unit]
    val healthCheck = CassandraHealthCheck.of[IO](0.seconds, 1.second, Resource.pure[IO, IO[Unit]](error))
    val result = for {
      error <- healthCheck.use(_.error.untilDefinedM)
    } yield {
      error shouldEqual error
    }
    result.run()
  }
}
