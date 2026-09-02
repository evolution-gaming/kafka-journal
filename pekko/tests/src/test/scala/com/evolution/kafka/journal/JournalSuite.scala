package com.evolution.kafka.journal

import cats.data.NonEmptyList as Nel
import cats.effect.*
import cats.effect.implicits.*
import cats.implicits.*
import com.evolution.kafka.journal.IOSuite.*
import com.evolution.kafka.journal.Journal.DataIntegrityConfig
import com.evolution.kafka.journal.conversions.{KafkaRead, KafkaWrite}
import com.evolution.kafka.journal.eventual.EventualRead
import com.evolution.kafka.journal.eventual.cassandra.EventualCassandra
import com.evolution.kafka.journal.pekko.persistence.KafkaJournalConfig
import com.evolution.kafka.journal.util.PureConfigHelper.*
import com.evolutiongaming.catshelper.{FromFuture, LogOf}
import org.scalatest.Suite
import org.scalatest.matchers.should.Matchers
import pureconfig.{ConfigReader, ConfigSource}

import java.time.Instant
import scala.concurrent.Promise

trait JournalSuite extends ActorSuite with Matchers { self: Suite =>

  import IntegrationTestInstances.*

  lazy val config: ConfigReader.Result[KafkaJournalConfig] = {
    ConfigSource
      .fromConfig(actorSystem.settings.config)
      .at("evolutiongaming.kafka-journal.persistence.journal")
      .load[KafkaJournalConfig]
  }

  lazy val ((eventualJournal, producer), release) = {
    implicit val logOf: LogOf[IO] = LogOf.empty
    val resource = for {
      config <- config.liftTo[IO].toResource
      origin <- Origin.hostName[IO].toResource
      eventualJournal <- EventualCassandra.make[IO](
        config.cassandra,
        origin,
        none,
        cassandraClusterOf,
        DataIntegrityConfig.Default,
      )
      producer <- Journals.Producer.make[IO](config.journal.kafka.producer)
    } yield {
      (eventualJournal, producer)
    }

    resource
      .allocated
      .unsafeRunSync()
  }

  private val await = Promise[Unit]()
  val awaitResources: IO[Unit] = FromFuture[IO].apply(await.future)

  override def beforeAll(): Unit = {
    super.beforeAll()
    IntegrationSuite.start()
    await.success {}
    //    eventual
    //    producer
  }

  override def afterAll(): Unit = {
    release.unsafeRunSync()
    super.afterAll()
  }
}

object JournalSuite {

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
        records <- journal.read().toList
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
