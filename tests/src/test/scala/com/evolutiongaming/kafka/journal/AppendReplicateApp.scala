package com.evolutiongaming.kafka.journal

import akka.actor.ActorSystem
import akka.persistence.kafka.journal.KafkaJournalConfig
import cats.Parallel
import cats.data.{NonEmptyList => Nel}
import cats.effect._
import cats.implicits._
import com.evolutiongaming.catshelper.ParallelHelper._
import com.evolutiongaming.catshelper.{FromFuture, FromTry, Log, LogOf, Runtime, ToFuture, ToTry}
import com.evolutiongaming.kafka.journal.eventual.EventualJournal
import com.evolutiongaming.kafka.journal.replicator.{Replicator, ReplicatorConfig}
import com.evolutiongaming.kafka.journal.util._
import com.evolutiongaming.kafka.journal.util.OptionHelper._
import com.evolutiongaming.scassandra.CassandraClusterOf
import com.evolutiongaming.scassandra.util.FromGFuture
import com.evolutiongaming.skafka.Topic
import com.evolutiongaming.smetrics.MeasureDuration
import com.typesafe.config.ConfigFactory

import scala.concurrent.duration._

object AppendReplicateApp extends IOApp {

  def run(args: List[String]): IO[ExitCode] = {
    val config = ConfigFactory.load("AppendReplicate.conf")
    val system = ActorSystem("AppendReplicateApp", config)
    implicit val ec = system.dispatcher
    implicit val timer = IO.timer(ec)
    implicit val parallel = IO.ioParallel
    implicit val measureDuration = MeasureDuration.fromClock(Clock[IO])

    val topic = "journal.AppendReplicate"

    val result = ActorSystemOf[IO](system).use { implicit system => runF[IO](topic) }
    result.as(ExitCode.Success)
  }


  private def runF[F[_] : ConcurrentEffect : Timer : Parallel : ContextShift : FromFuture : ToFuture : Runtime : FromGFuture : MeasureDuration : FromTry : ToTry](
    topic: Topic)(implicit
    system: ActorSystem,
  ): F[Unit] = {

    implicit val logOf = LogOfFromAkka[F](system)
    implicit val randomId = RandomId.uuid[F]

    val kafkaJournalConfig = Sync[F].delay {
      val config = system.settings.config.getConfig("evolutiongaming.kafka-journal.persistence.journal")
      KafkaJournalConfig(config)
    }

    def journal(
      config: JournalConfig,
      hostName: Option[HostName],
      log: Log[F])(implicit
      kafkaConsumerOf: KafkaConsumerOf[F],
      kafkaProducerOf: KafkaProducerOf[F]
    ) = {

      val consumer = Journal.Consumer.of[F](config.consumer, config.pollTimeout)
      for {
        producer <- Journal.Producer.of[F](config.producer)
      } yield {
        Journal[F](
          origin = hostName.map(Origin.fromHostName),
          producer = producer,
          consumer = consumer,
          eventualJournal = EventualJournal.empty[F],
          headCache = HeadCache.empty[F],
          log = log)
      }
    }

    def replicator(hostName: Option[HostName])(implicit kafkaConsumerOf: KafkaConsumerOf[F]) = {
      val config = Sync[F].delay {
        val config = system.settings.config.getConfig("evolutiongaming.kafka-journal.replicator")
        ReplicatorConfig(config)
      }
      for {
        cassandraClusterOf <- Resource.liftF(CassandraClusterOf.of[F])
        config             <- Resource.liftF(config)
        result             <- Replicator.of[F](config, cassandraClusterOf, hostName)
      } yield result
    }

    val resource = for {
      log                <- Resource.liftF(LogOf[F].apply(Journal.getClass))
      kafkaJournalConfig <- Resource.liftF(kafkaJournalConfig)
      blocking           <- Executors.blocking[F]("kafka-journal-blocking")
      kafkaConsumerOf     = KafkaConsumerOf[F](blocking)
      kafkaProducerOf     = KafkaProducerOf[F](blocking)
      hostName           <- Resource.liftF(HostName.of[F]())
      replicate          <- replicator(hostName)(kafkaConsumerOf)
      journal            <- journal(kafkaJournalConfig.journal, hostName, log)(kafkaConsumerOf, kafkaProducerOf)
    } yield {
      (journal, replicate)
    }

    resource.use { case (journal, replicate) =>
      Concurrent[F].race(append[F](topic, journal), replicate).void
    }
  }


  private def append[F[_] : Concurrent : Timer : Parallel](topic: Topic, journal: Journal[F]) = {

    def append(id: String) = {

      def append(seqNr: SeqNr) = {
        val key = Key(id = id, topic = topic)
        val event = Event(seqNr, payload = Some(Payload("AppendReplicateApp")))

        for {
          _      <- journal.append(key, Nel.of(event))
          result <- seqNr.next[Option].fold(().asRight[SeqNr].pure[F]) { seqNr =>
            for {
              _ <- Timer[F].sleep(100.millis)
            } yield {
              seqNr.asLeft[Unit]
            }
          }
        } yield result
      }

      SeqNr.min.tailRecM(append)
    }

    (0 to 10).toList.parFoldMap { id => append(id.toString) }
  }
}
