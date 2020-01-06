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

    val commonConfig = CommonConfig(
      clientId = "KafkaHealthCheckApp".some,
      bootstrapServers = Nel.of("localhost:9092"))
    val kafkaConfig = KafkaConfig(
      ProducerConfig(common = commonConfig),
      ConsumerConfig(common = commonConfig))

    val kafkaHealthCheck = KafkaHealthCheck.of[F](
      KafkaHealthCheck.Config.default,
      kafkaConfig)

    kafkaHealthCheck.use(_.error.untilDefinedM)
  }
}
