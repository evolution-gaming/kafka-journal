package com.evolution.kafka.journal.app

import cats.data.NonEmptyList as Nel
import cats.effect.*
import cats.effect.implicits.*
import cats.implicits.*
import com.evolution.kafka.journal.*
import com.evolution.kafka.journal.TestJsonCodec.instance
import com.evolution.kafka.journal.conversions.KafkaWrite
import com.evolution.kafka.journal.eventual.EventualJournal
import com.evolution.kafka.journal.pekko.persistence.KafkaJournalConfig
import com.evolution.kafka.journal.replicator.{Replicator, ReplicatorConfig}
import com.evolution.kafka.journal.util.PureConfigHelper.*
import com.evolutiongaming.catshelper.*
import com.evolutiongaming.catshelper.ParallelHelper.*
import com.evolutiongaming.scassandra.CassandraClusterOf
import com.evolutiongaming.skafka.Topic
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.pekko.actor.ActorSystem
import pureconfig.ConfigSource

import scala.concurrent.duration.*

object AppendReplicateApp extends IOApp {
  import cats.effect.unsafe.implicits.global

  private val topic = "journal.AppendReplicate"

  def run(args: List[String]): IO[ExitCode] = {
    for {
      config <- IO { ConfigFactory.load("AppendReplicateApp.conf") }
      _ <- makeActorSystemResource(config).use(runIo)
    } yield ExitCode.Success
  }

  private def makeActorSystemResource(config: Config): ResourceIO[ActorSystem] = {
    Resource.make(IO {
      ActorSystem("AppendReplicateApp", config)
    }) { system =>
      IO.fromFuture(IO {
        system.terminate()
      }).void
    }
  }

  private def runIo(system: ActorSystem): IO[Unit] = {

    implicit val logOf: LogOf[IO] = LogOfFromPekko[IO](system)
    implicit val randomIdOf: RandomIdOf[IO] = RandomIdOf.uuid

    val kafkaJournalConfig = ConfigSource
      .fromConfig(system.settings.config)
      .at("evolutiongaming.kafka-journal.persistence.journal")
      .load[KafkaJournalConfig]
      .liftTo[IO]

    def journal(
      config: JournalConfig,
      hostName: Option[HostName],
      log: Log[IO],
    )(implicit
      kafkaConsumerOf: KafkaConsumerOf[IO],
      kafkaProducerOf: KafkaProducerOf[IO],
    ) = {

      for {
        producer <- Journals.Producer.make[IO](config.kafka.producer)
      } yield {
        Journals(
          origin = hostName.map(Origin.fromHostName),
          producer = producer,
          consumer = Journals.Consumer.make[IO](config.kafka.consumer, config.pollTimeout),
          eventualJournal = EventualJournal.empty[IO],
          headCache = HeadCache.empty[IO],
          log = log,
          conversionMetrics = none,
        )
      }
    }

    def replicator(
      hostName: Option[HostName],
    )(implicit
      kafkaConsumerOf: KafkaConsumerOf[IO],
    ) = {
      for {
        cassandraClusterOf <- CassandraClusterOf.of[IO].toResource
        config <- ReplicatorConfig.fromConfig[IO](system.settings.config).toResource
        result <- Replicator.make[IO](config, cassandraClusterOf, hostName)
      } yield result
    }

    val resource = for {
      log <- LogOf[IO].apply(Journals.getClass).toResource
      kafkaJournalConfig <- kafkaJournalConfig.toResource
      kafkaConsumerOf = KafkaConsumerOf[IO]()
      kafkaProducerOf = KafkaProducerOf[IO]()
      hostName <- HostName.of[IO]().toResource
      replicate <- replicator(hostName)(kafkaConsumerOf)
      journal <- journal(kafkaJournalConfig.journal, hostName, log)(kafkaConsumerOf, kafkaProducerOf)
    } yield {
      (journal, replicate)
    }

    resource.use {
      case (journal, replicate) =>
        IO.race(append(topic, journal), replicate).void
    }
  }

  private def append(
    topic: Topic,
    journals: Journals[IO],
  )(implicit
    kafkaWrite: KafkaWrite[IO, Payload],
  ) = {

    def append(id: String) = {

      def append(seqNr: SeqNr) = {
        val key = Key(id = id, topic = topic)
        val event = Event(seqNr, payload = Payload("AppendReplicateApp").some)

        for {
          _ <- journals(key).append(Nel.of(event))
          result <- seqNr.next[Option].fold(().asRight[SeqNr].pure[IO]) { seqNr =>
            for {
              _ <- IO.sleep(100.millis)
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
