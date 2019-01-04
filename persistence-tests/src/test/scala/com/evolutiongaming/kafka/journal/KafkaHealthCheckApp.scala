package com.evolutiongaming.kafka.journal

import cats.effect.{ExitCode, IO, IOApp}
import cats.implicits._
import com.evolutiongaming.kafka.journal.util.FromFuture
import com.evolutiongaming.nel.Nel
import com.evolutiongaming.skafka.CommonConfig
import com.evolutiongaming.skafka.consumer.ConsumerConfig
import com.evolutiongaming.skafka.producer.ProducerConfig

import scala.concurrent.ExecutionContext

object KafkaHealthCheckApp extends IOApp {

  def run(args: List[String]): IO[ExitCode] = {

    implicit val ec = ExecutionContext.global
    implicit val fromFuture = FromFuture.lift[IO]

    val consumerConfig = ConsumerConfig(common = CommonConfig(
      bootstrapServers = Nel("localhost:9092"),
      clientId = Some("KafkaHealthCheckApp")))

    val producerConfig = ProducerConfig(
      common = consumerConfig.common)

    val kafkaHealthCheck = KafkaHealthCheck.of[IO](
      config = KafkaHealthCheck.Config.Default,
      producerConfig = producerConfig,
      consumerConfig = consumerConfig,
      blocking = ec)

    for {
      _ <- kafkaHealthCheck.use(_.error.untilDefinedM)
    } yield {
      ExitCode.Success
    }
  }
}
