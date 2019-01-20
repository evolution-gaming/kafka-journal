package com.evolutiongaming.kafka.journal

import akka.actor.ActorSystem
import akka.persistence.kafka.journal.KafkaJournalConfig
import cats.effect._
import cats.implicits._
import com.evolutiongaming.kafka.journal.eventual.EventualJournal
import com.evolutiongaming.kafka.journal.replicator.{Replicator, ReplicatorConfig}
import com.evolutiongaming.kafka.journal.util.ClockHelper._
import com.evolutiongaming.kafka.journal.util._
import com.evolutiongaming.nel.Nel
import com.evolutiongaming.skafka.Topic
import com.typesafe.config.ConfigFactory

import scala.concurrent.duration._

object AppendReplicateApp extends IOApp {

  def run(args: List[String]): IO[ExitCode] = {
    val config = ConfigFactory.load("AppendReplicate.conf")
    val system = ActorSystem("AppendReplicateApp", config)
    implicit val ec = system.dispatcher
    implicit val timer = IO.timer(ec)
    implicit val fromFuture = FromFuture.lift[IO]
    implicit val toFuture = ToFuture.io
    implicit val parallel = IO.ioParallel
    implicit val par = Par.lift
    implicit val runtime = Runtime.lift[IO]

    val topic = "journal.AppendReplicate"

    val result = ActorSystemOf[IO](system).use { implicit system => runF[IO](topic) }
    result.as(ExitCode.Success)
  }


  private def runF[F[_] : Concurrent : Timer : Par : ContextShift : FromFuture : ToFuture : Runtime](
    topic: Topic)(implicit
    system: ActorSystem): F[Unit] = {

    implicit val logOf = LogOf[F](system)

    val kafkaJournalConfig = Sync[F].delay {
      val config = system.settings.config.getConfig("evolutiongaming.kafka-journal.persistence.journal")
      KafkaJournalConfig(config)
    }

    def journal(
      producer: KafkaProducer[F],
      journalConfig: JournalConfig)(implicit
      kafkaConsumerOf: KafkaConsumerOf[F]) = {

      val consumer = Journal.Consumer.of[F](journalConfig.consumer)
      for {
        log <- LogOf[F].apply(Journal.getClass)
      } yield {
        implicit val log1 = log
        Journal[F](
          origin = Origin.HostName,
          kafkaProducer = producer,
          consumer = consumer,
          eventualJournal = EventualJournal.empty[F],
          pollTimeout = journalConfig.pollTimeout,
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
        result <- Replicator.of[F](config)
      } yield result
    }

    val resource = for {
      kafkaJournalConfig <- Resource.liftF(kafkaJournalConfig)
      blocking           <- Executors.blocking[F]
      kafkaConsumerOf     = KafkaConsumerOf[F](blocking)
      kafkaProducerOf     = KafkaProducerOf[F](blocking)
      replicate          <- replicator(kafkaConsumerOf)
      producer           <- kafkaProducerOf.apply(kafkaJournalConfig.journal.producer)
      journal            <- Resource.liftF(journal(producer, kafkaJournalConfig.journal)(kafkaConsumerOf))
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
          timestamp <- Clock[F].instant
          _         <- journal.append(key, Nel(event), timestamp)
          result    <- seqNr.next.fold(().asRight[SeqNr].pure[F]) { seqNr =>
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

    Par[F].foldMap((0 to 10).toList) { id => append(id.toString) }
  }
}
