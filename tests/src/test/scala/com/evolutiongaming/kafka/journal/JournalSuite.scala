package com.evolutiongaming.kafka.journal


import java.time.Instant

import akka.persistence.kafka.journal.KafkaJournalConfig
import cats.data.{NonEmptyList => Nel}
import cats.Monad
import cats.implicits._
import cats.effect.{Clock, IO, Resource}
import com.evolutiongaming.catshelper.LogOf
import com.evolutiongaming.kafka.journal.eventual.cassandra.EventualCassandra
import com.evolutiongaming.kafka.journal.IOSuite._
import com.evolutiongaming.kafka.journal.CassandraSuite._
import com.evolutiongaming.skafka.consumer.ConsumerMetrics
import com.evolutiongaming.skafka.producer.ProducerMetrics
import org.scalatest.Suite
import org.scalatest.matchers.should.Matchers
import play.api.libs.json.JsValue


trait JournalSuite extends ActorSuite with Matchers { self: Suite =>

  lazy val config: KafkaJournalConfig = {
    val config = actorSystem.settings.config.getConfig("evolutiongaming.kafka-journal.persistence.journal")
    KafkaJournalConfig(config)
  }

  implicit val kafkaConsumerOf: KafkaConsumerOf[IO] = KafkaConsumerOf[IO](
    actorSystem.dispatcher,
    ConsumerMetrics.empty[IO].some)

  implicit val kafkaProducerOf: KafkaProducerOf[IO] = KafkaProducerOf[IO](
    actorSystem.dispatcher,
    ProducerMetrics.empty[IO].some)

  implicit val randomIdOf: RandomIdOf[IO] = RandomIdOf.uuid[IO]

  lazy val ((eventualJournal, producer), release) = {
    implicit val logOf = LogOf.empty[IO]
    val resource = for {
      origin          <- Resource.liftF(Origin.hostName[IO])
      eventualJournal <- EventualCassandra.of[IO](config.cassandra, origin, none, cassandraClusterOf)
      producer        <- Journal.Producer.of[IO](config.journal.producer)
    } yield {
      (eventualJournal, producer)
    }

    resource.allocated.unsafeRunSync()
  }

  override def beforeAll() = {
    super.beforeAll()
    IntegrationSuite.start()
//    eventual
//    producer
  }

  override def afterAll() = {
    release.unsafeRunSync()
    super.afterAll()
  }
}

object JournalSuite {

  // TODO expiry: move out from tests
  trait KeyJournal[F[_]] {

    def append(
      events: Nel[Event],
      metadata: Option[JsValue] = none,
      headers: Headers = Headers.empty,
      expireAfter: Option[ExpireAfter] = none
    ): F[PartitionOffset]

    def read: F[List[EventRecord]]

    def pointer: F[Option[SeqNr]]

    def delete(to: DeleteTo): F[Option[PartitionOffset]]

    def purge: F[Option[PartitionOffset]]

    def size: F[Long]
  }

  object KeyJournal {

    def apply[F[_] : Monad : Clock](
      key: Key,
      timestamp: Instant,
      journal: Journal[F]
    ): KeyJournal[F] = new KeyJournal[F] {

      def append(
        events: Nel[Event],
        metadata: Option[JsValue],
        headers: Headers,
        expireAfter: Option[ExpireAfter]
      ) = {
        journal.append(
          key = key,
          events = events,
          expireAfter = expireAfter,
          metadata = metadata,
          headers = headers)
      }

      def read = {
        for {
          records <- journal.read(key).toList
        } yield for {
          record <- records
        } yield {
          record.copy(timestamp = timestamp)
        }
      }

      def pointer = journal.pointer(key)

      def delete(to: DeleteTo) = journal.delete(key, to)

      def purge = journal.purge(key)

      def size = journal.read(key).length
    }
  }
}