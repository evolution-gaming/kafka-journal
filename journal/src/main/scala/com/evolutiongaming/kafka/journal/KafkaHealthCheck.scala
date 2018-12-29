package com.evolutiongaming.kafka.journal

import java.util.UUID

import cats.Applicative
import cats.effect.concurrent.Ref
import cats.effect.{Concurrent, Sync, Timer}
import cats.implicits._
import com.evolutiongaming.kafka.journal.util.ClockHelper._
import com.evolutiongaming.kafka.journal.util.{ClockOf, FromFuture, TimerOf}
import com.evolutiongaming.nel.Nel
import com.evolutiongaming.skafka.consumer.{Consumer, ConsumerConfig}
import com.evolutiongaming.skafka.producer.{Producer, ProducerConfig, ProducerRecord}
import com.evolutiongaming.skafka.{CommonConfig, Topic}

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future, TimeoutException}
import scala.util.control.NoStackTrace

trait KafkaHealthCheck[F[_]] {
  def error: F[Option[Throwable]]
  def close: F[Unit]
}

object KafkaHealthCheck {

  def empty[F[_] : Applicative]: KafkaHealthCheck[F] = new KafkaHealthCheck[F] {
    def error = none[Throwable].pure[F]
    def close = ().pure[F]
  }

  def of[F[_] : Concurrent : Timer : FromFuture](
    bootstrapServers: Nel[String],
    ecBlocking: ExecutionContext): F[KafkaHealthCheck[F]] = {

    for {
      log <- Log.of[F](KafkaHealthCheck.getClass)
      key <- Sync[F].delay { UUID.randomUUID().toString }
      topic = "healthcheck"
      clientId = "replicator"
      groupId = s"$clientId-$topic-$key"
      commonConfig = CommonConfig(
        bootstrapServers = bootstrapServers,
        clientId = Some(clientId))
      consumerConfig = ConsumerConfig(common = commonConfig, groupId = Some(groupId))
      consumer <- Sync[F].delay {
        val consumer = Consumer[String, String](consumerConfig, ecBlocking)
        consumer.subscribe(Nel(topic), None)
        consumer
      }
      producerConfig = ProducerConfig(common = commonConfig, retries = 3)
      producer <- Sync[F].delay { Producer(producerConfig, ecBlocking) }
      result <- of[F](
        key = key,
        topic = topic,
        interval = 1.second,
        timeout = 10.seconds,
        producer = producer,
        consumer = consumer,
        log = log)
    } yield result
  }

  def of[F[_] : Concurrent : Timer : FromFuture](
    key: String,
    topic: Topic,
    interval: FiniteDuration,
    timeout: FiniteDuration,
    producer: Producer[Future],
    consumer: Consumer[String, String, Future],
    log: Log[F]): F[KafkaHealthCheck[F]] = {

    implicit val clock = TimerOf[F].clock
    implicit val log1 = log

    for {
      stateRef <- Ref.of[F, State](State(None, stop = false))
      fiber <- Concurrent[F].start {

        def timeoutFailure[A](msg: String) = {
          Sync[F].raiseError[A](new TimeoutException(msg) with NoStackTrace)
        }

        def poll(id: String, deadline: Long): F[Unit] = {
          val poll: F[Option[Unit]] = for {
            now <- ClockOf[F].millis
            result <- {
              if (now > deadline) timeoutFailure(s"timed out in $timeout finding $id")
              else {
                for {
                  records <- FromFuture[F].apply { consumer.poll(100.millis) }
                  values = records.values.values.flatten
                  found = values.find { record =>
                    record.key.exists(_.value == key) && record.value.exists(_.value == id)
                  }
                } yield found.void
              }
            }
          } yield result

          poll.untilDefinedM
        }

        val produce = {
          for {
            id <- Sync[F].delay { UUID.randomUUID().toString }
            record = ProducerRecord[String, String](topic = topic, value = Some(id), key = Some(key))
            _ <- FromFuture[F].apply { producer.send(record) }
          } yield id
        }

        def produceConsume(deadline: Long) = {
          for {
            result <- {
              for {
                id <- produce
                _ <- poll(id, deadline)
              } yield {}
            }.attempt // TODO redeem
          } yield {
            result.fold(_.some, _ => none)
          }
        }

        val poll1: F[Option[Unit]] = {

          for {
            now <- ClockOf[F].millis
            deadline = now + timeout.toMillis
            timeoutFinal = timeout + 1.second

            timeoutF = for {
              _ <- TimerOf[F].sleep(timeoutFinal)
              // TODO redeem
              result <- timeoutFailure[Unit](s"timed out in $timeoutFinal").attempt.map[Option[Throwable]](_.fold[Option[Throwable]](_.some, _ => none[Throwable])) /*TODO*/
            } yield {
              result
            }
            result <- Concurrent[F].race(produceConsume(deadline), timeoutF)
            result1 = result.merge
            _ <- result1.fold(().pure[F]) { failure => Log[F].error(s"failed $failure", failure) }
            stop <- stateRef.modify { state =>
              val result = state.copy(failure = result1)
              (result, result.stop)
            }

            result <- {
              if (stop) ().some.pure[F]
              else {
                for {
                  _ <- TimerOf[F].sleep(interval)
                } yield none[Unit]
              }
            }
          } yield {
            result
          }
        }

        for {
          _ <- TimerOf[F].sleep(10.seconds)
          _ <- FromFuture[F].apply { consumer.poll(3.seconds) }
          _ <- produce
          _ <- FromFuture[F].apply { consumer.poll(3.seconds) }
          _ <- poll1.untilDefinedM
        } yield {}
      }
    } yield {

      new KafkaHealthCheck[F] {

        def error = {
          for {
            state <- stateRef.get
          } yield {
            state.failure
          }
        }

        def close = {
          for {
            _ <- stateRef.update { _.copy(stop = true) }
            _ <- fiber.join
            _ <- FromFuture[F].apply { consumer.close() }
            _ <- FromFuture[F].apply { producer.close() }
          } yield {}
        }
      }
    }
  }


  final case class State(failure: Option[Throwable], stop: Boolean)
}
