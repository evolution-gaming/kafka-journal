package com.evolutiongaming.kafka.journal

import java.util.UUID

import cats.effect._
import cats.effect.concurrent.Ref
import cats.implicits._
import cats.{Applicative, FlatMap}
import com.evolutiongaming.kafka.journal.util.CatsHelper._
import com.evolutiongaming.kafka.journal.util.FromFuture
import com.evolutiongaming.skafka.Topic
import com.evolutiongaming.skafka.consumer.{ConsumerConfig, ConsumerRecords}
import com.evolutiongaming.skafka.producer.{ProducerConfig, ProducerRecord}

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

trait KafkaHealthCheck[F[_]] {
  def error: F[Option[Throwable]]
}

object KafkaHealthCheck {

  def empty[F[_] : Applicative]: KafkaHealthCheck[F] = new KafkaHealthCheck[F] {
    def error = none[Throwable].pure[F]
  }

  def of[F[_] : Concurrent : Timer : FromFuture : ContextShift](
    config: Config,
    producerConfig: ProducerConfig,
    consumerConfig: ConsumerConfig,
    blocking: ExecutionContext): Resource[F, KafkaHealthCheck[F]] = {

    val randomId = Sync[F].delay { UUID.randomUUID().toString } // TODO use RNG

    val result = for {
      log <- Log.of[F](KafkaHealthCheck.getClass)
      key <- randomId
    } yield {
      implicit val log1 = log

      val consumerConfig1 = {
        val groupId = consumerConfig.common.clientId.fold(key) { clientId => s"$clientId-$key" }
        consumerConfig.copy(groupId = Some(groupId))
      }

      val producer = for {
        producer <- KafkaProducer.of(producerConfig, blocking, None)
      } yield {
        Producer(key = key, topic = config.topic, producer = producer)
      }

      val consumer = for {
        consumer <- KafkaConsumer.of[F, String, String](consumerConfig1, blocking, None)
      } yield {
        Consumer(consumer)
      }

      of(
        key = key,
        config = config,
        randomId = randomId,
        producer = producer,
        consumer = consumer)
    }

    Resource.liftF(result).flatten
  }

  def of[F[_] : Concurrent : Timer : ContextShift : Log](
    key: String,
    config: Config,
    randomId: F[String],
    producer: Resource[F, Producer[F]],
    consumer: Resource[F, Consumer[F]]): Resource[F, KafkaHealthCheck[F]] = {

    Resource {
      for {
        ref   <- Ref.of[F, Option[Throwable]](None)
        fiber <- (producer, consumer).tupled.fork { case (producer, consumer) =>

          def poll(id: String) = {
            for {
              records <- consumer.poll(config.pollTimeout)
              values   = records.values.values.flatten
              found    = values.find { record =>
                record.key.exists(_.value == key) && record.value.exists(_.value == id)
              }
            } yield found.void
          }

          val produce = for {
            id <- randomId
            _  <- producer.send(id)
          } yield id

          val produceConsume = for {
            id <- produce
            _  <- poll(id).untilDefinedM
          } yield {}

          val check: F[Unit] = {

            for {
              e <- produceConsume
                .timeoutFixed(config.timeout)
                .redeemWith[Option[Throwable], Throwable] { e =>
                Log[F].error(s"failed with $e", e).as(e.some)
              } { _ =>
                none[Throwable].pure[F]
              }
              _ <- ref.set(e)
              _ <- Timer[F].sleep(config.interval)
            } yield {}
          }

          for {
            _ <- Timer[F].sleep(config.initial)
            _ <- consumer.subscribe(config.topic)
            _ <- consumer.poll(1.second)
            _ <- produce
            _ <- check.foreverM[Unit]
          } yield {}
        }
      } yield {

        val result = new KafkaHealthCheck[F] {
          def error = ref.get
        }

        (result, fiber.cancel)
      }
    }
  }


  trait Producer[F[_]] {
    def send(value: String): F[Unit]
  }

  object Producer {

    def apply[F[_] : FlatMap](
      key: String,
      topic: Topic,
      producer: KafkaProducer[F]): Producer[F] = {

      new Producer[F] {
        def send(value: String) = {
          val record = ProducerRecord[String, String](topic = topic, value = Some(value), key = Some(key))
          producer.send(record).void
        }
      }
    }
  }


  trait Consumer[F[_]] {

    def subscribe(topic: Topic): F[Unit]
    
    def poll(timeout: FiniteDuration): F[ConsumerRecords[String, String]]
  }

  object Consumer {

    def apply[F[_] : FromFuture](consumer: KafkaConsumer[F, String, String]): Consumer[F] = {

      new Consumer[F] {

        def subscribe(topic: Topic) = {
          consumer.subscribe(topic)
        }

        def poll(timeout: FiniteDuration) = {
          consumer.poll(timeout)
        }
      }
    }
  }


  final case class Config(
    topic: Topic = "healthcheck",
    initial: FiniteDuration = 10.seconds,
    interval: FiniteDuration = 1.second,
    timeout: FiniteDuration = 10.seconds,
    pollTimeout: FiniteDuration = 100.millis)

  object Config {
    val Empty: Config = Config()
  }
}
