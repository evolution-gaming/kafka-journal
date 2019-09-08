package com.evolutiongaming.kafka.journal

import cats.effect._
import cats.effect.concurrent.Ref
import cats.implicits._
import cats.{Applicative, Functor, Monad}
import com.evolutiongaming.catshelper.{FromTry, Log, LogOf}
import com.evolutiongaming.kafka.journal.CatsHelper._
import com.evolutiongaming.skafka.Topic
import com.evolutiongaming.skafka.consumer.{AutoOffsetReset, ConsumerConfig}
import com.evolutiongaming.skafka.producer.{ProducerConfig, ProducerRecord}

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
  

  def of[F[_] : Concurrent : ContextShift : Timer : LogOf : KafkaConsumerOf : KafkaProducerOf : RandomId : FromTry](
    config: Config,
    producerConfig: ProducerConfig,
    consumerConfig: ConsumerConfig
  ): Resource[F, KafkaHealthCheck[F]] = {

    val result = for {
      log <- LogOf[F].apply(KafkaHealthCheck.getClass)
      key <- RandomId[F].get
    } yield {
      implicit val log1 = log

      val consumer = Consumer.of[F](key, consumerConfig)
      
      val producer = Producer.of[F](config.topic, producerConfig)

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
    consumer: Resource[F, Consumer[F]]
  ): Resource[F, KafkaHealthCheck[F]] = {

    val result = for {
      ref   <- Ref.of[F, Option[Throwable]](None)
      fiber <- Concurrent[F].start {
        (producer, consumer).tupled.use { case (producer, consumer) =>
          run(key, config, stop, producer, consumer, ref.set)
        }
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

  def run[F[_] : Concurrent : Timer : ContextShift : Log](
    key: String,
    config: Config,
    stop: F[Boolean],
    producer: Producer[F],
    consumer: Consumer[F],
    set: Option[Throwable] => F[Unit]
  ): F[Unit] = {

    val sleep = Timer[F].sleep(config.interval)

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
            for {
              _ <- ContextShift[F].shift
              _ <- sleep
              _ <- produce(s"$n:$retry")
            } yield {
              (retry + 1).asLeft[Unit]
            }
          } { _ =>
            ().asRight[Long].pure[F]
          }
        } yield result
      }

      val produceConsume = for {
        _ <- produce(value)
        _ <- 0l.tailRecM(consume)
      } yield {}

      produceConsume
        .timeout(config.timeout)
        .error
    }

    def check(n: Long) = {
      for {
        error  <- produceConsume(n)
        _      <- error.fold(().pure[F]) { error => Log[F].error(s"$n failed with $error") }
        _      <- set(error)
        _      <- sleep
        stop   <- stop
        result <- {
          if (stop) ().asRight[Long].pure[F]
          else for {
            _ <- ContextShift[F].shift
          } yield {
            (n + 1).asLeft[Unit]
          }
        }
      } yield result
    }

    for {
      _ <- Timer[F].sleep(config.initial)
      _ <- consumer.subscribe(config.topic)
      _ <- consumer.poll(config.interval)
      _ <- produceConsume(0l) // warmup
      _ <- 1l.tailRecM(check)
    } yield {}
  }


  trait Producer[F[_]] {
    def send(record: Record): F[Unit]
  }

  object Producer {

    def apply[F[_]](implicit F: Producer[F]): Producer[F] = F

    def apply[F[_] : Monad : FromTry](topic: Topic, producer: KafkaProducer[F]): Producer[F] = {
      new Producer[F] {
        def send(record: Record) = {
          val record1 = ProducerRecord[String, String](topic = topic, key = record.key, value = record.value)
          producer.send(record1).void
        }
      }
    }

    def of[F[_] : Sync : KafkaProducerOf : FromTry](topic: Topic, config: ProducerConfig): Resource[F, Producer[F]] = {
      for {
        producer <- KafkaProducerOf[F].apply(config)
      } yield {
        Producer[F](topic = topic, producer = producer)
      }
    }
  }


  trait Consumer[F[_]] {

    def subscribe(topic: Topic): F[Unit]
    
    def poll(timeout: FiniteDuration): F[Iterable[Record]]
  }

  object Consumer {

    def apply[F[_]](implicit F: Consumer[F]): Consumer[F] = F

    def apply[F[_] : Functor](consumer: KafkaConsumer[F, String, String]): Consumer[F] = {

      new Consumer[F] {

        def subscribe(topic: Topic) = {
          consumer.subscribe(topic)
        }

        def poll(timeout: FiniteDuration) = {
          for {
            records <- consumer.poll(timeout)
          } yield for {
            record <- records.values.values.flatMap(_.toList)
          } yield {
            Record(key = record.key.map(_.value), value = record.value.map(_.value))
          }
        }
      }
    }

    def of[F[_] : Sync : KafkaConsumerOf : FromTry](key: String, config: ConsumerConfig): Resource[F, Consumer[F]] = {
      val config1 = {
        val groupId = config.common.clientId.fold(key) { clientId => s"$clientId-$key" }
        config.copy(
          groupId = Some(groupId),
          autoOffsetReset = AutoOffsetReset.Latest)
      }

      for {
        consumer <- KafkaConsumerOf[F].apply[String, String](config1)
      } yield {
        Consumer[F](consumer)
      }
    }
  }


  final case class Record(key: Option[String], value: Option[String])


  final case class Config(
    topic: Topic = "healthcheck",
    initial: FiniteDuration = 10.seconds,
    interval: FiniteDuration = 1.second,
    timeout: FiniteDuration = 2.minutes,
    pollTimeout: FiniteDuration = 10.millis)

  object Config {
    val Default: Config = Config()
  }
}
