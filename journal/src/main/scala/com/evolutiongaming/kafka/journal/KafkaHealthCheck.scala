package com.evolutiongaming.kafka.journal

import java.util.UUID

import cats.effect._
import cats.effect.concurrent.Ref
import cats.implicits._
import cats.{Applicative, FlatMap}
import com.evolutiongaming.kafka.journal.util.CatsHelper._
import com.evolutiongaming.kafka.journal.util.FromFuture
import com.evolutiongaming.skafka.Topic
import com.evolutiongaming.skafka.consumer.{AutoOffsetReset, ConsumerConfig}
import com.evolutiongaming.skafka.producer.{ProducerConfig, ProducerRecord}

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

trait KafkaHealthCheck[F[_]] {
  def error: F[Option[Throwable]]
  def done: F[Unit]
}

object KafkaHealthCheck {

  def empty[F[_] : Applicative]: KafkaHealthCheck[F] = new KafkaHealthCheck[F] {
    def error = none[Throwable].pure[F]
    def done = ().pure[F]
  }

  def of[F[_] : Concurrent : Timer : FromFuture : ContextShift](
    config: Config,
    producerConfig: ProducerConfig,
    consumerConfig: ConsumerConfig,
    blocking: ExecutionContext): Resource[F, KafkaHealthCheck[F]] = {

    val result = for {
      log <- Log.of[F](KafkaHealthCheck.getClass)
      key <- Sync[F].delay { UUID.randomUUID().toString }
    } yield {
      implicit val log1 = log

      val consumerConfig1 = {
        val groupId = consumerConfig.common.clientId.fold(key) { clientId => s"$clientId-$key" }
        consumerConfig.copy(
          groupId = Some(groupId),
          autoOffsetReset = AutoOffsetReset.Latest)
      }

      val producer = for {
        producer <- KafkaProducer.of(producerConfig, blocking, None)
      } yield {
        Producer(topic = config.topic, producer = producer)
      }

      val consumer = for {
        consumer <- KafkaConsumer.of[F, String, String](consumerConfig1, blocking, None)
      } yield {
        Consumer(consumer)
      }

      of(
        key = key,
        config = config,
        stop = false.pure[F],
        producer = producer,
        consumer = consumer)
    }

    Resource.liftF(result).flatten
  }

  def of[F[_] : Concurrent : Timer : ContextShift : Log](
    key: String,
    config: Config,
    stop: F[Boolean],
    producer: Resource[F, Producer[F]],
    consumer: Resource[F, Consumer[F]]): Resource[F, KafkaHealthCheck[F]] = {

    val result = for {
      ref   <- Ref.of[F, Option[Throwable]](None)
      fiber <- (producer, consumer).tupled.start { case (producer, consumer) =>

        def produce(value: String) = {
          val record = Record(key = Some(key), value = Some(value))
          for {
            _ <- Log[F].debug(s"$key send $value")
            _ <- producer.send(record)
          } yield {}
        }

        def produceConsume(n: Long) = {
          val value = n.toString

          def consume(retry: Long) = {
            for {
              records <- consumer.poll(config.pollTimeout)
              found    = records.find { record => record.key.contains(key) && record.value.contains(value) }
              result  <- found.fold {
                produce(s"$n, retry: $retry").as((retry + 1).asLeft[Unit])
              } { _ =>
                ().asRight[Long].pure[F]
              }
            } yield {
              result
            }
          }

          val produceConsume = for {
            _ <- produce(value)
            _ <- 0l.tailRecM(consume)
          } yield {}

          produceConsume
            .timeout1(config.timeout)
            .error
        }

        def check(n: Long) = {
          for {
            error <- produceConsume(n)
            _     <- error.fold(().pure[F]) { error => Log[F].error(s"$n failed with $error", error) }
            _     <- ref.set(error)
            _     <- Timer[F].sleep(config.interval)
            stop  <- stop
          } yield {
            if (stop) ().asRight[Long]
            else (n + 1).asLeft[Unit]
          }
        }

        for {
          _ <- Timer[F].sleep(config.initial)
          _ <- consumer.subscribe(config.topic)
          _ <- consumer.poll(config.interval)
          _ <- produceConsume(0l) // warmup
          _ <- 1l.tailRecM(check)
        } yield {}
      }
    } yield {

      val result = new KafkaHealthCheck[F] {
        def error = ref.get
        def done = fiber.join
      }

      (result, fiber.cancel)
    }

    Resource(result)
  }


  trait Producer[F[_]] {
    def send(record: Record): F[Unit]
  }

  object Producer {

    def apply[F[_]](implicit F: Producer[F]): Producer[F] = F

    def apply[F[_] : FlatMap](topic: Topic, producer: KafkaProducer[F]): Producer[F] = {
      new Producer[F] {
        def send(record: Record) = {
          val record1 = ProducerRecord[String, String](topic = topic, key = record.key, value = record.value)
          producer.send(record1).void
        }
      }
    }
  }


  trait Consumer[F[_]] {

    def subscribe(topic: Topic): F[Unit]
    
    def poll(timeout: FiniteDuration): F[Iterable[Record]]
  }

  object Consumer {

    def apply[F[_]](implicit F: Consumer[F]): Consumer[F] = F

    def apply[F[_] : FlatMap : FromFuture](consumer: KafkaConsumer[F, String, String]): Consumer[F] = {

      new Consumer[F] {

        def subscribe(topic: Topic) = {
          consumer.subscribe(topic)
        }

        def poll(timeout: FiniteDuration) = {
          for {
            records <- consumer.poll(timeout)
          } yield for {
            record <- records.values.values.flatten
          } yield {
            Record(key = record.key.map(_.value), value = record.value.map(_.value))
          }
        }
      }
    }
  }


  final case class Record(key: Option[String], value: Option[String])


  final case class Config(
    topic: Topic = "healthcheck",
    initial: FiniteDuration = 10.seconds,
    interval: FiniteDuration = 1.second,
    timeout: FiniteDuration = 10.seconds,
    pollTimeout: FiniteDuration = 100.millis)

  object Config {
    val Default: Config = Config()
  }
}
