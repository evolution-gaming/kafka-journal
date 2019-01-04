package com.evolutiongaming.kafka.journal

import cats.data.StateT
import cats.effect._
import cats.implicits._
import com.evolutiongaming.kafka.journal.KafkaHealthCheck.Record
import com.evolutiongaming.kafka.journal.util.ConcurrentOf
import com.evolutiongaming.kafka.journal.util.IOSuite._
import com.evolutiongaming.skafka.Topic
import org.scalatest.{AsyncFunSuite, Matchers}

import scala.concurrent.duration._
import scala.util.control.NoStackTrace

class KafkaHealthCheckSpec extends AsyncFunSuite with Matchers {
  import KafkaHealthCheckSpec.DataF._
  import KafkaHealthCheckSpec._

  test("error") {
    implicit val log = Log.empty[IO]

    val producer = new KafkaHealthCheck.Producer[IO] {
      def send(record: Record) = ().pure[IO]
    }

    val consumer = new KafkaHealthCheck.Consumer[IO] {

      def subscribe(topic: Topic) = ().pure[IO]

      def poll(timeout: FiniteDuration) = {
        if (timeout == 1.second) List.empty[Record].pure[IO]
        else Error.raiseError[IO, List[Record]]
      }
    }

    val healthCheck = KafkaHealthCheck.of[IO](
      key = "key",
      config = KafkaHealthCheck.Config(
        topic = "topic",
        initial = 0.seconds,
        interval = 1.second),
      stop = false.pure[IO],
      producer = Resource.pure[IO, KafkaHealthCheck.Producer[IO]](producer),
      consumer = Resource.pure[IO, KafkaHealthCheck.Consumer[IO]](consumer))
    val result = for {
      error <- healthCheck.use(_.error.untilDefinedM)
    } yield {
      error shouldEqual error
    }
    result.run()
  }

  test("periodic healthcheck") {
    implicit val concurrent = ConcurrentOf[DataF]
    val stop = DataF { data =>
      val data1 = data.copy(checks = data.checks - 1)
      (data1, data1.checks <= 0)
    }
    val healthCheck = KafkaHealthCheck.of[DataF](
      key = "key",
      config = KafkaHealthCheck.Config(
        topic = "topic",
        initial = 0.millis,
        interval = 0.millis,
        timeout = 100.millis),
      stop = stop,
      producer = Resource.pure[DataF, KafkaHealthCheck.Producer[DataF]](KafkaHealthCheck.Producer[DataF]),
      consumer = Resource.pure[DataF, KafkaHealthCheck.Consumer[DataF]](KafkaHealthCheck.Consumer[DataF]))

    val initial = Data(checks = 2)
    val result = for {
      ab <- healthCheck.use(_.done).run(initial)
    } yield {
      val (data, _) = ab
      data shouldEqual Data(
        subscribed = Some("topic"),
        logs = List(
          "debug key send 2:0",
          "debug key send 2",
          "debug key send 1:0",
          "debug key send 1",
          "debug key send 0:0",
          "debug key send 0"))
    }

    result.run()
  }
}

object KafkaHealthCheckSpec {

  type DataF[A] = StateT[IO, Data, A]

  val Error: Throwable = new RuntimeException with NoStackTrace

  object DataF {

    implicit val LogDataF: Log[DataF] = {

      def add(log: String) = DataF[Unit] { data =>
        val data1 = data.copy(logs = log :: data.logs)
        (data1, ())
      }

      new Log[DataF] {
        def debug(msg: => String) = add(s"debug $msg")
        def info(msg: => String) = add(s"info $msg")
        def warn(msg: => String) = add(s"warn $msg")
        def error(msg: => String) = add(s"error $msg")
        def error(msg: => String, cause: Throwable) = add(s"error $msg $cause")
      }
    }


    implicit val ConsumerDataF: KafkaHealthCheck.Consumer[DataF] = new KafkaHealthCheck.Consumer[DataF] {

      def subscribe(topic: Topic) = {
        DataF { data =>
          val data1 = data.copy(subscribed = Some(topic))
          (data1, ())
        }
      }

      def poll(timeout: FiniteDuration) = {
        DataF { data =>
          if (data.records.size >= 2) {
            (data.copy(records = List.empty), data.records)
          } else {
            (data, List.empty)
          }
        }
      }
    }


    implicit val ProducerDataF: KafkaHealthCheck.Producer[DataF] = new KafkaHealthCheck.Producer[DataF] {

      def send(record: Record) = {
        StateT[IO, Data, Unit] { data =>
          val data1 = data.copy(records = record :: data.records)
          (data1, ()).pure[IO]
        }
      }
    }

    def apply[A](f: Data => (Data, A)): DataF[A] = StateT[IO, Data, A](data => f(data).pure[IO])
  }


  final case class Data(
    checks: Int = 0,
    subscribed: Option[Topic] = None,
    logs: List[String] = List.empty,
    records: List[Record] = List.empty)
}
