package com.evolutiongaming.kafka.journal


import java.time.Instant

import akka.persistence.kafka.journal.KafkaJournalConfig
import cats.Monad
import cats.implicits._
import cats.effect.{Clock, IO}
import com.evolutiongaming.catshelper.LogOf
import com.evolutiongaming.kafka.journal.eventual.cassandra.EventualCassandra
import com.evolutiongaming.kafka.journal.IOSuite._
import com.evolutiongaming.kafka.journal.CassandraSuite._
import com.evolutiongaming.nel.Nel
import com.evolutiongaming.skafka.consumer.ConsumerMetrics
import com.evolutiongaming.skafka.producer.ProducerMetrics
import org.scalatest.{Matchers, Suite}
import play.api.libs.json.JsValue


trait JournalSuite extends ActorSuite with Matchers { self: Suite =>

  lazy val config: KafkaJournalConfig = {
    val config = system.settings.config.getConfig("evolutiongaming.kafka-journal.persistence.journal")
    KafkaJournalConfig(config)
  }

  implicit val kafkaConsumerOf: KafkaConsumerOf[IO] = KafkaConsumerOf[IO](
    system.dispatcher,
    ConsumerMetrics.empty[IO].some)

  implicit val kafkaProducerOf: KafkaProducerOf[IO] = KafkaProducerOf[IO](
    system.dispatcher,
    ProducerMetrics.empty[IO].some)

  implicit val randomId: RandomId[IO] = RandomId.uuid[IO]

  lazy val ((eventual, producer), release) = {
    implicit val logOf = LogOf.empty[IO]
    val resource = for {
      eventualJournal <- EventualCassandra.of[IO](config.cassandra, None, cassandraClusterOf)
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

  trait KeyJournal[F[_]] {

    def append(events: Nel[Event], metadata: Option[JsValue] = None, headers: Headers = Headers.Empty): F[PartitionOffset]

    def read: F[List[EventRecord]]

    def size: F[Long]

    def pointer: F[Option[SeqNr]]

    def delete(to: SeqNr): F[Option[PartitionOffset]]
  }

  object KeyJournal {

    def apply[F[_] : Monad : Clock](
      key: Key,
      timestamp: Instant,
      journal: Journal[F]
    ): KeyJournal[F] = new KeyJournal[F] {

      def append(events: Nel[Event], metadata: Option[JsValue], headers: Headers) = {
        journal.append(key, events, metadata, headers)
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

      def size = journal.read(key).length

      def pointer = journal.pointer(key)

      def delete(to: SeqNr) = journal.delete(key, to)
    }
  }
}