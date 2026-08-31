package com.evolution.kafka.journal.demo

import cats.Parallel
import cats.data.NonEmptyList as Nel
import cats.effect.*
import cats.effect.syntax.resource.*
import cats.syntax.all.*
import com.evolution.kafka.journal.*
import com.evolution.kafka.journal.conversions.KafkaWrite
import com.evolution.kafka.journal.eventual.EventualJournal
import com.evolution.kafka.journal.replicator.{Replicator, ReplicatorConfig}
import com.evolution.kafka.journal.util.*
import com.evolution.kafka.journal.util.PureConfigHelper.*
import com.evolutiongaming.catshelper.*
import com.evolutiongaming.catshelper.ParallelHelper.*
import com.evolutiongaming.retry.Sleep
import com.evolutiongaming.scassandra.CassandraClusterOf
import com.evolutiongaming.scassandra.util.FromGFuture
import com.evolutiongaming.skafka.Topic
import com.typesafe.config.{Config, ConfigFactory}
import pureconfig.ConfigSource

import scala.concurrent.duration.*

object AppendReplicateApp extends IOApp {

  private implicit def jsonCodec[F[_]: ApplicativeThrowable: FromTry]: JsonCodec[F] = JsonCodec.default[F]

  def run(args: List[String]): IO[ExitCode] = {
    import cats.effect.unsafe.implicits.global

    val config = ConfigFactory.load("AppendReplicateApp.conf")
    implicit val measureDuration: MeasureDuration[IO] = MeasureDuration.fromClock(Clock[IO])

    val topic = "journal.AppendReplicate"

    for {
      logOf <- LogOf.slf4j[IO]
      _ <- {
        implicit val logOf1: LogOf[IO] = logOf
        runF[IO](topic, config)
      }
    } yield ExitCode.Success
  }

  private def runF[F[_]: Async: Parallel: FromGFuture: MeasureDuration: FromAttempt: FromTry: ToTry: Fail: LogOf](
    topic: Topic,
    config: Config,
  ): F[Unit] = {

    implicit val randomIdOf: RandomIdOf[F] = RandomIdOf.uuid[F]

    val journalConfig = ConfigSource
      .fromConfig(config)
      .at("evolutiongaming.kafka-journal.persistence.journal")
      .load[JournalConfig]
      .liftTo[F]

    def journal(
      config: JournalConfig,
      hostName: Option[HostName],
      log: Log[F],
    )(implicit
      kafkaConsumerOf: KafkaConsumerOf[F],
      kafkaProducerOf: KafkaProducerOf[F],
    ) = {

      for {
        producer <- Journals.Producer.make[F](config.kafka.producer)
      } yield {
        Journals[F](
          origin = hostName.map(Origin.fromHostName),
          producer = producer,
          consumer = Journals.Consumer.make[F](config.kafka.consumer, config.pollTimeout),
          eventualJournal = EventualJournal.empty[F],
          headCache = HeadCache.empty[F],
          log = log,
          conversionMetrics = none,
        )
      }
    }

    def replicator(
      hostName: Option[HostName],
    )(implicit
      kafkaConsumerOf: KafkaConsumerOf[F],
    ) = {
      for {
        cassandraClusterOf <- CassandraClusterOf.of[F].toResource
        config <- ReplicatorConfig.fromConfig[F](config).toResource
        result <- Replicator.make[F](config, cassandraClusterOf, hostName)
      } yield result
    }

    val resource = for {
      log <- LogOf[F].apply(Journals.getClass).toResource
      journalConfig <- journalConfig.toResource
      kafkaConsumerOf = KafkaConsumerOf[F]()
      kafkaProducerOf = KafkaProducerOf[F]()
      hostName <- HostName.of[F]().toResource
      replicate <- replicator(hostName)(kafkaConsumerOf)
      journal <- journal(journalConfig, hostName, log)(kafkaConsumerOf, kafkaProducerOf)
    } yield {
      (journal, replicate)
    }

    resource.use {
      case (journal, replicate) =>
        Concurrent[F].race(append[F](topic, journal), replicate).void
    }
  }

  private def append[F[_]: Concurrent: Sleep: Parallel](
    topic: Topic,
    journals: Journals[F],
  )(implicit
    kafkaWrite: KafkaWrite[F, Payload],
  ) = {

    def append(id: String) = {

      def append(seqNr: SeqNr) = {
        val key = Key(id = id, topic = topic)
        val event = Event(seqNr, payload = Payload("AppendReplicateApp").some)

        for {
          _ <- journals(key).append(Nel.of(event))
          result <- seqNr.next[Option].fold(().asRight[SeqNr].pure[F]) { seqNr =>
            for {
              _ <- Sleep[F].sleep(100.millis)
            } yield {
              seqNr.asLeft[Unit]
            }
          }
        } yield result
      }

      SeqNr.min.tailRecM(append)
    }

    (0 to 10)
      .toList
      .parFoldMap1 { id => append(id.toString) }
  }
}
