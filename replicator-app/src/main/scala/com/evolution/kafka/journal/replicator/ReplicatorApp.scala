package com.evolution.kafka.journal.replicator

import cats.effect.*
import cats.effect.unsafe.IORuntime
import com.evolution.kafka.journal.*
import com.evolution.kafka.journal.util.Fail
import com.evolutiongaming.catshelper.*
import com.evolutiongaming.scassandra.CassandraClusterOf
import com.evolutiongaming.scassandra.util.FromGFuture
import com.typesafe.config.ConfigFactory

/**
 * Standalone entry point running a single [[Replicator]] instance against the Kafka and Cassandra
 * clusters configured under `evolutiongaming.kafka-journal.replicator` in `application.conf`.
 */
object ReplicatorApp extends IOApp {

  override def run(args: List[String]): IO[ExitCode] = {

    implicit val ioRuntime: IORuntime = runtime
    implicit val fromTry: FromTry[IO] = FromTry.lift[IO]
    implicit val toTry: ToTry[IO] = ToTry.ioToTry
    implicit val fail: Fail[IO] = Fail.lift[IO]
    implicit val fromGFuture: FromGFuture[IO] = FromGFuture.lift1[IO]
    implicit val measureDuration: MeasureDuration[IO] = MeasureDuration.fromClock(Clock[IO])
    implicit val jsonCodec: JsonCodec[IO] = JsonCodec.default[IO]

    LogOf.slf4j[IO].flatMap { implicit logOf =>
      implicit val kafkaConsumerOf: KafkaConsumerOf[IO] = KafkaConsumerOf[IO]()
      for {
        log <- logOf(ReplicatorApp.getClass)
        config <- ReplicatorConfig.fromConfig[IO](ConfigFactory.load())
        hostName <- HostName.of[IO]()
        cassandraClusterOf <- CassandraClusterOf.of[IO]
        _ <- Replicator
          .make[IO](config, cassandraClusterOf, hostName)
          .use { done => log.info("replicator started") *> done }
      } yield ExitCode.Success
    }
  }
}
