package com.evolutiongaming.kafka.journal

import java.time.Instant
import java.util.UUID

import akka.actor.ActorSystem
import akka.persistence.kafka.journal.KafkaJournalConfig
import com.evolutiongaming.concurrent.async.Async
import com.evolutiongaming.kafka.journal.AsyncHelper._
import com.evolutiongaming.kafka.journal.eventual.{EventualJournal, ReplicatedJournal}
import com.evolutiongaming.kafka.journal.replicator.{ReplicatorConfig, TopicReplicator}
import com.evolutiongaming.nel.Nel
import com.evolutiongaming.safeakka.actor.ActorLog
import com.evolutiongaming.skafka.CommonConfig
import com.evolutiongaming.skafka.consumer.Consumer
import com.evolutiongaming.skafka.producer.{Acks, Producer, ProducerConfig}

import scala.concurrent.Future

object AppendReplicate extends App {

  val name = getClass.getName
  val topic = "append-replicate"
  implicit val system = ActorSystem(topic)
  implicit val ec = system.dispatcher
  val log = ActorLog(system, getClass)

  val commonConfig = CommonConfig(
    clientId = Some(topic),
    bootstrapServers = Nel("localhost:9092", "localhost:9093", "localhost:9094"))

  val producer = {
    val config = ProducerConfig(
      common = commonConfig,
      acks = Acks.All,
      retries = 100,
      idempotence = true)
    Producer(config, ec)
  }

  val journal = {
    val config = {
      val config = system.settings.config.getConfig("evolutiongaming.kafka-journal.persistence.journal")
      KafkaJournalConfig(config)
    }
    val ecBlocking = system.dispatchers.lookup(config.blockingDispatcher)
    val producer = Producer(config.journal.producer, ecBlocking)
    val topicConsumer = TopicConsumer(config.journal.consumer, ecBlocking)

    Journal(
      producer = producer,
      origin = Some(Origin(topic)),
      topicConsumer = topicConsumer,
      eventual = EventualJournal.empty,
      pollTimeout = config.journal.pollTimeout,
      closeTimeout = config.journal.closeTimeout,
      headCache = HeadCache.empty)
  }

  def append(id: String) = {

    val key = Key(id = id, topic = topic)

    def append(seqNr: SeqNr): Future[Unit] = {
      val event = Event(seqNr, payload = Some(Payload(name)))
      for {
        _ <- journal.append(key, Nel(event), Instant.now()).future
        _ <- seqNr.next.fold(Future.unit)(append)
      } yield ()
    }

    val result = append(SeqNr.Min)
    result.failed.foreach { failure => log.error(s"producer $key: $failure", failure) }
    result
  }

  def consume(nr: Int) = {
    val topicReplicator = {
      val config = {
        val config = system.settings.config.getConfig("evolutiongaming.kafka-journal.replicator")
        ReplicatorConfig(config)
      }
      val ecBlocking = system.dispatchers.lookup(config.blockingDispatcher)
      val consumer = Consumer[Id, Bytes](config.consumer, ecBlocking)
      val kafkaConsumer = KafkaConsumer[Async](consumer, config.pollTimeout)
      TopicReplicator(topic, ReplicatedJournal.empty, kafkaConsumer, TopicReplicator.Metrics.empty)
    }

    val result = topicReplicator.done().future
    result.failed.foreach { failure => log.error(s"consumer $nr: $failure", failure) }
    result
  }

  val producers = for {
    _ <- 1 to 3
  } yield append(UUID.randomUUID().toString)

  val consumers = for {
    n <- 1 to 3
  } yield consume(n)

  val result = Future.sequence(consumers ++ producers)
  result.onComplete { _ => system.terminate() }
}
