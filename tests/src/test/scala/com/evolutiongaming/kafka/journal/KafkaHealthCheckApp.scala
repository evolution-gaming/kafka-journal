package com.evolutiongaming.kafka.journal

import cats.data.{NonEmptyList => Nel}
import cats.effect._
import cats.syntax.all._
import com.evolutiongaming.catshelper.{FromTry, LogOf, MeasureDuration, ToFuture, ToTry}
import com.evolutiongaming.skafka.CommonConfig
import com.evolutiongaming.skafka.consumer.ConsumerConfig
import com.evolutiongaming.skafka.producer.ProducerConfig

object KafkaHealthCheckApp extends IOApp {

  def run(args: List[String]): IO[ExitCode] = {

    implicit val measureDuration = MeasureDuration.empty[IO]
    import cats.effect.unsafe.implicits.global

    for {
      logOf <- LogOf.slf4j[IO]
      _ <- {
        implicit val logOf1 = logOf
        runF[IO]
      }
    } yield {
      ExitCode.Success
    }
  }

  private def runF[F[_]: Async: ToFuture: LogOf: FromTry: ToTry: MeasureDuration] = {

    implicit val kafkaConsumerOf = KafkaConsumerOf[F]()

    implicit val kafkaProducerOf = KafkaProducerOf[F]()

    implicit val randomIdOf = RandomIdOf.uuid[F]

    val commonConfig = CommonConfig(clientId = "KafkaHealthCheckApp".some, bootstrapServers = Nel.of("localhost:9092"))
    val kafkaConfig  = KafkaConfig(ProducerConfig(common = commonConfig), ConsumerConfig(common = commonConfig))

    val kafkaHealthCheck = KafkaHealthCheck.of[F](KafkaHealthCheck.Config.default, kafkaConfig)

    kafkaHealthCheck.use(_.error.untilDefinedM)
  }
}
