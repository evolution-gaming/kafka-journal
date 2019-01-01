package com.evolutiongaming.kafka.journal

import cats.effect.{IO, Resource}
import cats.implicits._
import com.evolutiongaming.kafka.journal.util.IOSuite._
import com.evolutiongaming.skafka.Topic
import com.evolutiongaming.skafka.consumer.ConsumerRecords
import org.scalatest.{AsyncFunSuite, Matchers}

import scala.concurrent.duration._
import scala.util.control.NoStackTrace

class KafkaHealthCheckSpec extends AsyncFunSuite with Matchers {

  test("KafkaHealthCheck") {
    implicit val log = Log.empty[IO]
    val error = new RuntimeException with NoStackTrace

    val producer = new KafkaHealthCheck.Producer[IO] {
      def send(value: String) = ().pure[IO]
    }

    val consumer = new KafkaHealthCheck.Consumer[IO] {

      def subscribe(topic: Topic) = ().pure[IO]
      
      def poll(timeout: FiniteDuration) = {
        if (timeout == 1.second) ConsumerRecords.empty[String, String].pure[IO]
        else error.raiseError[IO, ConsumerRecords[String, String]]
      }
    }

    val healthCheck = KafkaHealthCheck.of[IO](
      key = "key",
      config = KafkaHealthCheck.Config(
        topic = "topic",
        initial = 0.seconds,
        interval = 1.second,
        timeout = 5.seconds),
      randomId = "id".pure[IO],
      producer = Resource.pure[IO, KafkaHealthCheck.Producer[IO]](producer),
      consumer = Resource.pure[IO, KafkaHealthCheck.Consumer[IO]](consumer))
    val result = for {
      error <- healthCheck.use(_.error.untilDefinedM)
    } yield {
      error shouldEqual error
    }
    result.run()
  }
}
