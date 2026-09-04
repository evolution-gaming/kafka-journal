package com.evolution.kafka.journal.it

import cats.data.NonEmptyList as Nel
import cats.effect.*
import cats.implicits.*
import com.evolution.kafka.journal.*
import com.evolution.kafka.journal.IOSuite.*
import com.evolution.kafka.journal.Journal.{ConsumerPoolConfig, DataIntegrityConfig}
import com.evolution.kafka.journal.conversions.{KafkaRead, KafkaWrite}
import com.evolution.kafka.journal.eventual.cassandra.EventualCassandra
import com.evolution.kafka.journal.eventual.{EventualJournal, EventualRead}
import com.evolutiongaming.catshelper.{Log, LogOf}
import org.scalatest.Suite
import org.scalatest.matchers.should.Matchers

import java.time.Instant

/**
 * Base trait for Kafka-Journal integration tests not using Akka and working with journal interfaces
 * below the Akka persistence plugin.
 */
private[it] trait JournalSuite extends SuiteScopedResources with Matchers with IntegrationTestOrigin { this: Suite =>

  import IntegrationTestInstances.*

  protected final val config: JournalItConfig = SharedItEnv.require().defaultConfig

  // Helpers for constructing Kafka-Journal component graphs in integration tests:

  protected final def eventualCassandraResource: ResourceIO[EventualJournal[IO]] = EventualCassandra.make[IO](
    config = config.eventualCassandra,
    origin = origin.some,
    metrics = none,
    cassandraClusterOf = cassandraClusterOf,
    dataIntegrity = DataIntegrityConfig.Default,
  )

  protected final def journalProducerResource: ResourceIO[Journals.Producer[IO]] =
    Journals.Producer.make[IO](config.journal.kafka.producer)

  protected final def journalConsumerPoolResource: ResourceIO[ResourceIO[Journals.Consumer[IO]]] = {
    val consumerResource = Journals.Consumer.make[IO](config.journal.kafka.consumer, config.journal.pollTimeout)
    ConsumerPool.make[IO](
      poolConfig = ConsumerPoolConfig.Default,
      metrics = none,
      consumer = consumerResource,
    )
  }

  protected final def makeHeadCacheResource(
    eventualJournal: EventualJournal[IO],
    useHeadCache: Boolean,
  ): ResourceIO[HeadCache[IO]] = {
    if (useHeadCache) {
      HeadCacheOf[IO](HeadCacheMetrics.empty[IO].some).apply(config.journal.kafka.consumer, eventualJournal)
    } else {
      Resource.pure[IO, HeadCache[IO]](HeadCache.empty[IO])
    }
  }

  protected final def makeJournals(
    producer: Journals.Producer[IO],
    consumerResource: ResourceIO[Journals.Consumer[IO]],
    eventualJournal: EventualJournal[IO],
    headCache: HeadCache[IO],
  ): Journals[IO] = {
    Journals[IO](
      producer = producer,
      origin = origin.some,
      consumer = consumerResource,
      eventualJournal = eventualJournal,
      headCache = headCache,
      log = JournalSuite.journalsLog,
      conversionMetrics = none,
    )
  }
}

private[it] object JournalSuite {
  import IntegrationTestInstances.*

  private[this] val log = LogOf[IO].apply(classOf[JournalSuite]).unsafeRunSync()
  private val journalsLog: Log[IO] = LogOf[IO].apply(classOf[Journals[IO]]).unsafeRunSync()

  /**
   * Test wrapper for `Journal[IO]` with an ability to override read records timestamp with the
   * provided value.
   */
  final class JournalTest(journal: Journal[IO], readRecordTimestampOverride: Option[Instant] = none) {

    def append[A](
      events: Nel[Event[A]],
      metadata: RecordMetadata = RecordMetadata.empty,
      headers: Headers = Headers.empty,
    )(implicit
      kafkaWrite: KafkaWrite[IO, A],
    ): IO[PartitionOffset] = {
      journal.append(events, metadata, headers)
    }

    def read[A](
      implicit
      kafkaRead: KafkaRead[IO, A],
      eventualRead: EventualRead[IO, A],
    ): IO[List[EventRecord[A]]] = {
      for {
        startTime <- IO.realTimeInstant
        records <- journal.read().toList
        endTime <- IO.realTimeInstant
        _ <- log.debug(s"journal read took ${ java.time.Duration.between(startTime, endTime) }")
      } yield {
        readRecordTimestampOverride.fold(records) { newTimestamp =>
          records.map(_.copy(timestamp = newTimestamp))
        }
      }
    }

    def pointer: IO[Option[SeqNr]] = {
      journal.pointer
    }

    def delete(to: DeleteTo): IO[Option[PartitionOffset]] = {
      journal.delete(to)
    }

    def purge: IO[Option[PartitionOffset]] = {
      journal.purge
    }

    def size[A](
      implicit
      kafkaRead: KafkaRead[IO, A],
      eventualRead: EventualRead[IO, A],
    ): IO[Long] = {
      journal.read().length
    }
  }
}
