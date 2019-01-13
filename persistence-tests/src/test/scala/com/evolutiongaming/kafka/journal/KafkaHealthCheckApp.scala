package com.evolutiongaming.kafka.journal

import cats.effect._
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

    for {
      logOf <- LogOf.slfj4[IO]()
      _     <- {
        implicit val logOf1 = logOf
        runF[IO](ec)
      }
    } yield {
      ExitCode.Success
    }
  }

  private def runF[F[_] : Concurrent : Timer : FromFuture : ContextShift : LogOf](
    blocking: ExecutionContext) = {

    implicit val kafkaConsumerOf = KafkaConsumerOf[F](blocking)

    implicit val kafkaProducerOf = KafkaProducerOf[F](blocking)

    val consumerConfig = ConsumerConfig(common = CommonConfig(
      bootstrapServers = Nel("localhost:9092"),
      clientId = Some("KafkaHealthCheckApp")))

    val producerConfig = ProducerConfig(
      common = consumerConfig.common)

    val kafkaHealthCheck = KafkaHealthCheck.of[F](
      config = KafkaHealthCheck.Config.Default,
      producerConfig = producerConfig,
      consumerConfig = consumerConfig)

    kafkaHealthCheck.use(_.error.untilDefinedM)
  }
}
