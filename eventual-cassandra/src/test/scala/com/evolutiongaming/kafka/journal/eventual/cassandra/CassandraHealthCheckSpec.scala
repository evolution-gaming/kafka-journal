package com.evolutiongaming.kafka.journal.eventual.cassandra

import cats.effect.IO
import cats.effect.syntax.all._
import cats.syntax.all._
import com.evolutiongaming.catshelper.Log
import com.evolutiongaming.kafka.journal.IOSuite._
import org.scalatest.funsuite.AsyncFunSuite

import scala.concurrent.duration._
import scala.util.control.NoStackTrace

class CassandraHealthCheckSpec extends AsyncFunSuite {

  test("CassandraHealthCheck#of(statement)") {
    
    val expectedError = new RuntimeException with NoStackTrace

    val healthCheck = CassandraHealthCheck.of[IO](
      initial = 0.seconds,
      interval = 1.second,
      statement = expectedError.raiseError[IO, Unit].pure[IO].toResource,
      log = Log.empty[IO]
    )

    val actualError = healthCheck.use(_.error.untilDefinedM)

    val program = actualError.map { actualError =>
      assert(actualError == expectedError)
    }

    program.run()
  }

}
