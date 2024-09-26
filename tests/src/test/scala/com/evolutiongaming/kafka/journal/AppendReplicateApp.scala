package com.evolutiongaming.kafka.journal

import akka.actor.ActorSystem
import akka.persistence.kafka.journal.KafkaJournalConfig
import cats.Parallel
import cats.data.NonEmptyList as Nel
import cats.effect.*
import cats.effect.syntax.resource.*
import cats.syntax.all.*
import com.evolutiongaming.catshelper.*
import com.evolutiongaming.catshelper.ParallelHelper.*
import com.evolutiongaming.kafka.journal.TestJsonCodec.instance
import com.evolutiongaming.kafka.journal.conversions.KafkaWrite
import com.evolutiongaming.kafka.journal.eventual.EventualJournal
import com.evolutiongaming.kafka.journal.replicator.{Replicator, ReplicatorConfig}
import com.evolutiongaming.kafka.journal.util.*
import com.evolutiongaming.kafka.journal.util.PureConfigHelper.*
import com.evolutiongaming.retry.Sleep
import com.evolutiongaming.scassandra.CassandraClusterOf
import com.evolutiongaming.scassandra.util.FromGFuture
import com.evolutiongaming.skafka.Topic
import com.typesafe.config.ConfigFactory
import pureconfig.ConfigSource

import scala.concurrent.duration.*

object AppendReplicateApp extends IOApp {

  def run(args: List[String]): IO[ExitCode] = {
    import cats.effect.unsafe.implicits.global

    val config                   = ConfigFactory.load("AppendReplicate.conf")
    val system                   = ActorSystem("AppendReplicateApp", config)
    implicit val measureDuration = MeasureDuration.fromClock(Clock[IO])

    val topic = "journal.AppendReplicate"

    val result = ActorSystemOf[IO](system).use { implicit system => runF[IO](topic) }
    result.as(ExitCode.Success)
  }

  private def runF[
    F[_]: Async: Parallel: ToFuture: FromGFuture: MeasureDuration: FromAttempt: FromTry: ToTry: Fail,
  ](topic: Topic)(implicit system: ActorSystem): F[Unit] = {

    implicit val logOf      = LogOfFromAkka[F](system)
    implicit val randomIdOf = RandomIdOf.uuid[F]

    val kafkaJournalConfig = ConfigSource
      .fromConfig(system.settings.config)
      .at("evolutiongaming.kafka-journal.persistence.journal")
      .load[KafkaJournalConfig]
      .liftTo[F]

    def journal(config: JournalConfig, hostName: Option[HostName], log: Log[F])(
      implicit kafkaConsumerOf: KafkaConsumerOf[F],
      kafkaProducerOf: KafkaProducerOf[F],
    ) = {

      for {
        producer <- Journals.Producer.make[F](config.kafka.producer)
      } yield {
        Journals[F](
          origin            = hostName.map(Origin.fromHostName),
          producer          = producer,
          consumer          = Journals.Consumer.make[F](config.kafka.consumer, config.pollTimeout),
          eventualJournal   = EventualJournal.empty[F],
          headCache         = HeadCache.empty[F],
          log               = log,
          conversionMetrics = none,
        )
      }
    }

    def replicator(hostName: Option[HostName])(implicit kafkaConsumerOf: KafkaConsumerOf[F]) = {
      for {
        cassandraClusterOf <- CassandraClusterOf.of[F].toResource
        config             <- ReplicatorConfig.fromConfig[F](system.settings.config).toResource
        result             <- Replicator.make[F](config, cassandraClusterOf, hostName)
      } yield result
    }

    val resource = for {
      log                <- LogOf[F].apply(Journals.getClass).toResource
      kafkaJournalConfig <- kafkaJournalConfig.toResource
      kafkaConsumerOf     = KafkaConsumerOf[F]()
      kafkaProducerOf     = KafkaProducerOf[F]()
      hostName           <- HostName.of[F]().toResource
      replicate          <- replicator(hostName)(kafkaConsumerOf)
      journal            <- journal(kafkaJournalConfig.journal, hostName, log)(kafkaConsumerOf, kafkaProducerOf)
    } yield {
      (journal, replicate)
    }

    resource.use {
      case (journal, replicate) =>
        Concurrent[F].race(append[F](topic, journal), replicate).void
    }
  }

  private def append[F[_]: Concurrent: Sleep: Parallel](topic: Topic, journals: Journals[F])(
    implicit kafkaWrite: KafkaWrite[F, Payload],
  ) = {

    def append(id: String) = {

      def append(seqNr: SeqNr) = {
        val key   = Key(id = id, topic = topic)
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
