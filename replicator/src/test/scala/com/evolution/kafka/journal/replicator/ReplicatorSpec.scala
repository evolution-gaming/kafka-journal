package com.evolution.kafka.journal.replicator

import cats.effect.{IO, Outcome, Resource}
import cats.syntax.all.*
import com.evolution.kafka.journal.IOSuite.*
import com.evolution.kafka.journal.replicator.Replicator.Consumer
import com.evolutiongaming.catshelper.LogOf
import com.evolutiongaming.skafka.Topic
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec

import scala.concurrent.duration.*
import scala.util.control.NoStackTrace

class ReplicatorSpec extends AsyncWordSpec with Matchers {

  "Replicator" should {

    "fail if any of replicators failed" in {

      implicit val logOf: LogOf[IO] = LogOf.empty[IO]

      val error = new RuntimeException with NoStackTrace

      val consumer = new Consumer[IO] {
        def topics: IO[Set[Topic]] = Set("journal").pure[IO]
      }

      val start =
        (_: Topic) =>
          Resource.pure[IO, IO[Outcome[IO, Throwable, Unit]]](Outcome.errored[IO, Throwable, Unit](error).pure[IO])
      val result = for {
        result <- Replicator
          .make[IO](
            config = Replicator.Config(topicDiscoveryInterval = 0.millis),
            consumer = Resource.pure[IO, Consumer[IO]](consumer),
            topicReplicatorOf = start,
          )
          .use(identity)
          .attempt
      } yield {
        result shouldEqual error.asLeft
      }

      result.run()
    }

    "ignore excluded topics even if they match prefixes" in {

      implicit val logOf: LogOf[IO] = LogOf.empty[IO]

      val consumer = new Consumer[IO] {
        def topics: IO[Set[Topic]] = Set("journal", "journal-excluded", "other").pure[IO]
      }

      val stop = new RuntimeException with NoStackTrace

      val result = for {
        log <- logOf(Replicator.getClass)
        started <- IO.ref(List.empty[Topic])
        result <- Replicator
          .start[IO](
            config = Replicator.Config(topicDiscoveryInterval = 0.millis, excludeTopics = List("journal-excluded")),
            consumer = consumer,
            start = (topic: Topic) => started.update(topic :: _),
            continue = started.get.flatMap { topics => stop.raiseError[IO, Unit].whenA(topics.nonEmpty) },
            log = log,
          )
          .attempt
        started <- started.get
      } yield {
        result shouldEqual stop.asLeft
        started shouldEqual List("journal")
      }

      result.run()
    }
  }
}
