package com.evolutiongaming.kafka.journal

import cats.effect._
import cats.data.{NonEmptyList => Nel}
import cats.implicits._
import com.evolutiongaming.catshelper.{FromFuture, FromTry, LogOf, ToFuture, ToTry}
import com.evolutiongaming.skafka.CommonConfig
import com.evolutiongaming.skafka.consumer.ConsumerConfig
import com.evolutiongaming.skafka.producer.ProducerConfig
import com.evolutiongaming.smetrics.MeasureDuration

import scala.concurrent.ExecutionContext

object KafkaHealthCheckApp extends IOApp {

  def run(args: List[String]): IO[ExitCode] = {

    implicit val executor = ExecutionContext.global
    implicit val measureDuration = MeasureDuration.empty[IO]

    for {
      logOf <- LogOf.slf4j[IO]
      _     <- {
        implicit val logOf1 = logOf
        runF[IO](executor)
      }
    } yield {
      ExitCode.Success
    }
  }

  private def runF[F[_] : ConcurrentEffect : Timer : FromFuture : ToFuture : ContextShift : LogOf : FromTry : ToTry : MeasureDuration](
    blocking: ExecutionContext
  ) = {

    implicit val kafkaConsumerOf = KafkaConsumerOf[F](blocking)

    implicit val kafkaProducerOf = KafkaProducerOf[F](blocking)

    implicit val randomIdOf = RandomIdOf.uuid[F]

    val consumerConfig = ConsumerConfig(
      common = CommonConfig(
        clientId = "KafkaHealthCheckApp".some,
        bootstrapServers = Nel.of("localhost:9092")))

    val producerConfig = ProducerConfig(
      common = consumerConfig.common)

    val kafkaHealthCheck = KafkaHealthCheck.of[F](
      config = KafkaHealthCheck.Config.default,
      producerConfig = producerConfig,
      consumerConfig = consumerConfig)

    kafkaHealthCheck.use(_.error.untilDefinedM)
  }
}
