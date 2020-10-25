package com.evolutiongaming.kafka.journal

import cats.effect._
import cats.syntax.all._
import com.evolutiongaming.catshelper.Log
import com.evolutiongaming.kafka.journal.KafkaHealthCheck.Record
import com.evolutiongaming.kafka.journal.util.ConcurrentOf
import com.evolutiongaming.kafka.journal.IOSuite._
import com.evolutiongaming.skafka.Topic
import org.scalatest.funsuite.AsyncFunSuite
import org.scalatest.matchers.should.Matchers

import scala.concurrent.duration._
import scala.util.control.NoStackTrace

class KafkaHealthCheckSpec extends AsyncFunSuite with Matchers {
  import KafkaHealthCheckSpec.StateT._
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
      consumer = Resource.pure[IO, KafkaHealthCheck.Consumer[IO]](consumer),
      log = log)
    val result = for {
      error <- healthCheck.use(_.error.untilDefinedM)
    } yield {
      error shouldEqual error
    }
    result.run()
  }

  test("periodic healthcheck") {
    implicit val concurrent = ConcurrentOf.fromAsync[StateT]
    val stop = StateT { data =>
      val data1 = data.copy(checks = data.checks - 1)
      (data1, data1.checks <= 0)
    }
    val healthCheck = KafkaHealthCheck.of[StateT](
      key = "key",
      config = KafkaHealthCheck.Config(
        topic = "topic",
        initial = 0.millis,
        interval = 0.millis,
        timeout = 100.millis),
      stop = stop,
      producer = Resource.pure[StateT, KafkaHealthCheck.Producer[StateT]](KafkaHealthCheck.Producer[StateT]),
      consumer = Resource.pure[StateT, KafkaHealthCheck.Consumer[StateT]](KafkaHealthCheck.Consumer[StateT]),
      log = log)

    val initial = State(checks = 2)
    val result = for {
      ab <- healthCheck.use(_.done).run(initial)
    } yield {
      val (data, _) = ab
      data shouldEqual State(
        subscribed = "topic".some,
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

  type StateT[A] = cats.data.StateT[IO, State, A]

  val Error: Throwable = new RuntimeException with NoStackTrace

  object StateT {

    val log: Log[StateT] = {

      def add(log: String) = StateT[Unit] { data =>
        val data1 = data.copy(logs = log :: data.logs)
        (data1, ())
      }

      new Log[StateT] {
        def debug(msg: => String) = add(s"debug $msg")
        def info(msg: => String) = add(s"info $msg")
        def warn(msg: => String) = add(s"warn $msg")
        def warn(msg: => String, cause: Throwable) = add(s"warn $msg $cause")
        def error(msg: => String) = add(s"error $msg")
        def error(msg: => String, cause: Throwable) = add(s"error $msg $cause")
      }
    }


    implicit val consumer: KafkaHealthCheck.Consumer[StateT] = new KafkaHealthCheck.Consumer[StateT] {

      def subscribe(topic: Topic) = {
        StateT { data =>
          val data1 = data.copy(subscribed = topic.some)
          (data1, ())
        }
      }

      def poll(timeout: FiniteDuration) = {
        StateT { data =>
          if (data.records.size >= 2) {
            (data.copy(records = List.empty), data.records)
          } else {
            (data, List.empty)
          }
        }
      }
    }


    implicit val producer: KafkaHealthCheck.Producer[StateT] = new KafkaHealthCheck.Producer[StateT] {

      def send(record: Record) = {
        cats.data.StateT[IO, State, Unit] { data =>
          val data1 = data.copy(records = record :: data.records)
          (data1, ()).pure[IO]
        }
      }
    }

    def apply[A](f: State => (State, A)): StateT[A] = cats.data.StateT[IO, State, A](data => f(data).pure[IO])
  }


  final case class State(
    checks: Int = 0,
    subscribed: Option[Topic] = None,
    logs: List[String] = List.empty,
    records: List[Record] = List.empty)
}
