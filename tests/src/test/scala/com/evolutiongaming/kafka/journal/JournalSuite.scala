package com.evolutiongaming.kafka.journal


import java.time.Instant

import akka.persistence.kafka.journal.KafkaJournalConfig
import cats.Monad
import cats.data.{NonEmptyList => Nel}
import cats.effect.{Clock, IO, Resource}
import cats.implicits._
import com.evolutiongaming.catshelper.LogOf
import com.evolutiongaming.kafka.journal.CassandraSuite._
import com.evolutiongaming.kafka.journal.IOSuite._
import com.evolutiongaming.kafka.journal.eventual.cassandra.EventualCassandra
import com.evolutiongaming.kafka.journal.util.PureConfigHelper._
import com.evolutiongaming.skafka.consumer.ConsumerMetrics
import com.evolutiongaming.skafka.producer.ProducerMetrics
import org.scalatest.Suite
import org.scalatest.matchers.should.Matchers
import pureconfig.{ConfigReader, ConfigSource}


trait JournalSuite extends ActorSuite with Matchers { self: Suite =>

  lazy val config: ConfigReader.Result[KafkaJournalConfig] = {
    ConfigSource
      .fromConfig(actorSystem.settings.config)
      .at("evolutiongaming.kafka-journal.persistence.journal")
      .load[KafkaJournalConfig]
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
      config          <- Resource.liftF(config.liftTo[IO])
      origin          <- Resource.liftF(Origin.hostName[IO])
      eventualJournal <- EventualCassandra.of[IO](config.cassandra, origin, none, cassandraClusterOf)
      producer        <- Journals.Producer.of[IO](config.journal.kafka.producer)
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

  trait JournalTest[F[_]] {

    def append(
      events: Nel[Event],
      metadata: RecordMetadata = RecordMetadata.empty,
      headers: Headers = Headers.empty
    ): F[PartitionOffset]

    def read: F[List[EventRecord]]

    def pointer: F[Option[SeqNr]]

    def delete(to: DeleteTo): F[Option[PartitionOffset]]

    def purge: F[Option[PartitionOffset]]

    def size: F[Long]
  }

  object JournalTest {

    def apply[F[_] : Monad : Clock](
      journal: Journal[F],
      timestamp: Instant
    ): JournalTest[F] = new JournalTest[F] {

      def append(events: Nel[Event], metadata: RecordMetadata, headers: Headers) = {
        journal.append(events, metadata, headers)
      }

      def read = {
        for {
          records <- journal.read().toList
        } yield for {
          record <- records
        } yield {
          record.copy(timestamp = timestamp)
        }
      }

      def pointer = journal.pointer

      def delete(to: DeleteTo) = journal.delete(to)

      def purge = journal.purge

      def size = journal.read().length
    }
  }
}