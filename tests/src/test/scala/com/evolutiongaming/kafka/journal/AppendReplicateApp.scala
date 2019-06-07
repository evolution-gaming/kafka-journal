package com.evolutiongaming.kafka.journal

import akka.actor.ActorSystem
import akka.persistence.kafka.journal.KafkaJournalConfig
import cats.effect._
import cats.implicits._
import cats.temp.par._
import com.evolutiongaming.catshelper.{FromFuture, Log, LogOf, Runtime, ToFuture}
import com.evolutiongaming.kafka.journal.eventual.EventualJournal
import com.evolutiongaming.kafka.journal.replicator.{Replicator, ReplicatorConfig}
import com.evolutiongaming.kafka.journal.util._
import com.evolutiongaming.kafka.journal.CatsHelper._
import com.evolutiongaming.nel.Nel
import com.evolutiongaming.skafka.Topic
import com.typesafe.config.ConfigFactory

import scala.concurrent.ExecutionContextExecutor
import scala.concurrent.duration._

object AppendReplicateApp extends IOApp {

  def run(args: List[String]): IO[ExitCode] = {
    val config = ConfigFactory.load("AppendReplicate.conf")
    val system = ActorSystem("AppendReplicateApp", config)
    implicit val ec = system.dispatcher
    implicit val timer = IO.timer(ec)
    implicit val parallel = IO.ioParallel

    val topic = "journal.AppendReplicate"

    val result = ActorSystemOf[IO](system).use { implicit system => runF[IO](topic) }
    result.as(ExitCode.Success)
  }


  private def runF[F[_] : Concurrent : Timer : Par : ContextShift : FromFuture : ToFuture : Runtime : FromGFuture](
    topic: Topic)(implicit
    system: ActorSystem,
    executor: ExecutionContextExecutor
  ): F[Unit] = {

    implicit val logOf = LogOfFromAkka[F](system)
    implicit val randomId = RandomId.uuid[F]

    val kafkaJournalConfig = Sync[F].delay {
      val config = system.settings.config.getConfig("evolutiongaming.kafka-journal.persistence.journal")
      KafkaJournalConfig(config)
    }

    def journal(
      config: JournalConfig)(implicit
      kafkaConsumerOf: KafkaConsumerOf[F],
      kafkaProducerOf: KafkaProducerOf[F],
      log: Log[F]
    ) = {

      val consumer = Journal.Consumer.of[F](config.consumer, config.pollTimeout)
      for {
        producer <- Journal.Producer.of[F](config.producer)
      } yield {
        Journal[F](
          origin = Origin.HostName,
          producer = producer,
          consumer = consumer,
          eventualJournal = EventualJournal.empty[F],
          headCache = HeadCache.empty[F])
      }
    }

    def replicator(implicit kafkaConsumerOf: KafkaConsumerOf[F]) = {
      val config = Sync[F].delay {
        val config = system.settings.config.getConfig("evolutiongaming.kafka-journal.replicator")
        ReplicatorConfig(config)
      }
      for {
        config <- Resource.liftF(config)
        result <- Replicator.of[F](config, executor)
      } yield result
    }

    val resource = for {
      log                <- Resource.liftF(LogOf[F].apply(Journal.getClass))
      kafkaJournalConfig <- Resource.liftF(kafkaJournalConfig)
      blocking           <- Executors.blocking[F]("kafka-journal-blocking")
      kafkaConsumerOf     = KafkaConsumerOf[F](blocking)
      kafkaProducerOf     = KafkaProducerOf[F](blocking)
      replicate          <- replicator(kafkaConsumerOf)
      journal            <- journal(kafkaJournalConfig.journal)(kafkaConsumerOf, kafkaProducerOf, log)
    } yield {
      (journal, replicate)
    }

    resource.use { case (journal, replicate) =>
      Concurrent[F].race(append[F](topic, journal), replicate).void
    }
  }


  private def append[F[_] : Concurrent : Timer : Par](topic: Topic, journal: Journal[F]) = {

    def append(id: Id) = {

      def append(seqNr: SeqNr) = {
        val key = Key(id = id, topic = topic)
        val event = Event(seqNr, payload = Some(Payload("AppendReplicateApp")))

        for {
          _      <- journal.append(key, Nel(event))
          result <- seqNr.next.fold(().asRight[SeqNr].pure[F]) { seqNr =>
            for {
              _ <- Timer[F].sleep(100.millis)
            } yield {
              seqNr.asLeft[Unit]
            }
          }
        } yield result
      }

      SeqNr.Min.tailRecM(append)
    }

    (0 to 10).toList.parFoldMap { id => append(id.toString) }
  }
}
