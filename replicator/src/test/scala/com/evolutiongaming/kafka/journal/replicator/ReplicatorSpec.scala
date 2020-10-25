package com.evolutiongaming.kafka.journal.replicator

import cats.effect.{IO, Resource}
import cats.syntax.all._
import com.evolutiongaming.catshelper.LogOf
import com.evolutiongaming.kafka.journal.replicator.Replicator.Consumer
import com.evolutiongaming.kafka.journal.IOSuite._
import com.evolutiongaming.skafka.Topic
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec

import scala.concurrent.duration._
import scala.util.control.NoStackTrace

class ReplicatorSpec extends AsyncWordSpec with Matchers {

  "Replicator" should {

    "fail if any of replicators failed" in {

      implicit val logOf = LogOf.empty[IO]

      val error = new RuntimeException with NoStackTrace

      val consumer = new Consumer[IO] {
        def topics = Set("journal").pure[IO]
      }

      val start = (_: Topic) => Resource.pure[IO, IO[Unit]](error.raiseError[IO, Unit])
      val result = for {
        result <- Replicator.of(
          Replicator.Config(topicDiscoveryInterval = 0.millis),
          Resource.pure[IO, Consumer[IO]](consumer),
          start).use(identity).attempt
      } yield {
        result shouldEqual error.asLeft
      }

      result.run()
    }
  }
}