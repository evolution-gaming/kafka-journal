package com.evolutiongaming.kafka.journal

import java.time.Instant
import java.util.UUID

import akka.actor.ActorSystem
import akka.persistence.kafka.journal.KafkaJournalConfig
import cats.effect.{IO, Resource}
import cats.implicits._
import cats.~>
import com.evolutiongaming.kafka.journal.eventual.{EventualJournal, ReplicatedJournal}
import com.evolutiongaming.kafka.journal.replicator.{ReplicatorConfig, TopicReplicator}
import com.evolutiongaming.kafka.journal.util.FromFuture
import com.evolutiongaming.nel.Nel
import com.evolutiongaming.safeakka.actor.ActorLog
import com.evolutiongaming.skafka.CommonConfig

import scala.concurrent.Future

object AppendReplicateApp extends App {

  val name = getClass.getName
  val topic = "append-replicate"
  implicit val system = ActorSystem(name)
  implicit val ec = system.dispatcher
  implicit val cs = IO.contextShift(ec)
  implicit val timer = IO.timer(ec)
  implicit val fromFuture = FromFuture.lift[IO]
  val log = ActorLog(system, getClass)

  val commonConfig = CommonConfig(
    clientId = Some(topic),
    bootstrapServers = Nel("localhost:9092", "localhost:9093", "localhost:9094"))

  val journalConfig = {
    val config = system.settings.config.getConfig("evolutiongaming.kafka-journal.persistence.journal")
    KafkaJournalConfig(config)
  }

  val blocking = system.dispatchers.lookup(journalConfig.blockingDispatcher)

  val systemRes = Resource.make[IO, ActorSystem] {
    IO.pure(system)
  } { system =>
    FromFuture[IO].apply {
      system.terminate()
    }.void
  }

  val resources = for {
    system <- systemRes
    producer <- KafkaProducer.of[IO](journalConfig.journal.producer, blocking)
  } yield {
    (system, producer)
  }

  implicit val replicatedJournal = ReplicatedJournal.empty[IO]

  implicit val metrics = TopicReplicator.Metrics.empty[IO]

  val result = resources.use { case (_, producer) =>

    val journal: Journal[Future] = {
      val topicConsumer = TopicConsumer[IO](journalConfig.journal.consumer, blocking)
      val journal = Journal[IO](
        log = ActorLog.empty,
        kafkaProducer = producer,
        origin = Some(Origin(topic)),
        topicConsumer = topicConsumer,
        eventualJournal = EventualJournal.empty[IO],
        pollTimeout = journalConfig.journal.pollTimeout,
        headCache = HeadCache.empty[IO])

      val toFuture = new (IO ~> Future) {
        def apply[A](fa: IO[A]) = fa.unsafeToFuture()
      }

      journal.mapK(toFuture)
    }

    def append(id: String) = {

      val key = Key(id = id, topic = topic)

      def append(seqNr: SeqNr): Future[Unit] = {
        val event = Event(seqNr, payload = Some(Payload(name)))
        for {
          _ <- journal.append(key, Nel(event), Instant.now())
          _ <- seqNr.next.fold(Future.unit)(append)
        } yield ()
      }

      val result = append(SeqNr.Min)
      result.failed.foreach { failure => log.error(s"producer $key: $failure", failure) }
      result
    }

    def consume(nr: Int) = {
      val config = {
        val config = system.settings.config.getConfig("evolutiongaming.kafka-journal.replicator")
        ReplicatorConfig(config)
      }
      val blocking = system.dispatchers.lookup(config.blockingDispatcher)

      val consumer = for {
        consumer <- KafkaConsumer.of[IO, Id, Bytes](config.consumer, blocking)
      } yield {
        TopicReplicator.Consumer[IO](consumer, config.pollTimeout)
      }

      val done = for {
        replicator <- TopicReplicator.of[IO](topic, consumer)
        done <- replicator.done
      } yield done

      val future = done.unsafeToFuture()

      future.failed.foreach { failure => log.error(s"consumer $nr: $failure", failure) }
      future
    }

    val producers = for {
      _ <- 1 to 3
    } yield append(UUID.randomUUID().toString)

    val consumers = for {
      n <- 1 to 3
    } yield consume(n)

    FromFuture[IO].apply {
      Future.sequence(consumers ++ producers)
    }
  }

  result.unsafeRunSync()
}
