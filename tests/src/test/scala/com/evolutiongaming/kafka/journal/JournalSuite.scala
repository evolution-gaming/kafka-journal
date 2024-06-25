package com.evolutiongaming.kafka.journal

import akka.persistence.kafka.journal.KafkaJournalConfig
import cats.Monad
import cats.data.{NonEmptyList => Nel}
import cats.effect.IO
import cats.syntax.all._
import com.evolutiongaming.catshelper.{FromFuture, LogOf}
import com.evolutiongaming.kafka.journal.CassandraSuite._
import com.evolutiongaming.kafka.journal.IOSuite._
import com.evolutiongaming.kafka.journal.conversions.{KafkaRead, KafkaWrite}
import com.evolutiongaming.kafka.journal.eventual.EventualRead
import com.evolutiongaming.kafka.journal.eventual.cassandra.EventualCassandra
import com.evolutiongaming.kafka.journal.util.PureConfigHelper._
import com.evolutiongaming.skafka.consumer.ConsumerMetrics
import com.evolutiongaming.skafka.producer.ProducerMetrics
import org.scalatest.Suite
import org.scalatest.matchers.should.Matchers
import pureconfig.{ConfigReader, ConfigSource}

import java.time.Instant
import scala.concurrent.Promise

trait JournalSuite extends ActorSuite with Matchers { self: Suite =>

  import cats.effect.unsafe.implicits.global

  lazy val config: ConfigReader.Result[KafkaJournalConfig] =
    ConfigSource
      .fromConfig(actorSystem.settings.config)
      .at("evolutiongaming.kafka-journal.persistence.journal")
      .load[KafkaJournalConfig]

  implicit val kafkaConsumerOf: KafkaConsumerOf[IO] = KafkaConsumerOf[IO](ConsumerMetrics.empty[IO].some)

  implicit val kafkaProducerOf: KafkaProducerOf[IO] = KafkaProducerOf[IO](ProducerMetrics.empty[IO].some)

  implicit val randomIdOf: RandomIdOf[IO] = RandomIdOf.uuid[IO]

  lazy val ((eventualJournal, producer), release) = {
    implicit val logOf     = LogOf.empty[IO]
    implicit val jsonCodec = JsonCodec.jsoniter[IO]
    val resource = for {
      config          <- config.liftTo[IO].toResource
      origin          <- Origin.hostName[IO].toResource
      eventualJournal <- EventualCassandra.of[IO](config.cassandra, origin, none, cassandraClusterOf)
      producer        <- Journals.Producer.of[IO](config.journal.kafka.producer)
    } yield (eventualJournal, producer)

    resource.allocated
      .unsafeRunSync()
  }

  private val await            = Promise[Unit]()
  val awaitResources: IO[Unit] = FromFuture[IO].apply(await.future)

  override def beforeAll() = {
    super.beforeAll()
    IntegrationSuite.start()
    await.success {}
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

    def append[A](
      events: Nel[Event[A]],
      metadata: RecordMetadata = RecordMetadata.empty,
      headers: Headers = Headers.empty,
    )(implicit kafkaWrite: KafkaWrite[F, A]): F[PartitionOffset]

    def read[A](implicit kafkaRead: KafkaRead[F, A], eventualRead: EventualRead[F, A]): F[List[EventRecord[A]]]

    def pointer: F[Option[SeqNr]]

    def delete(to: DeleteTo): F[Option[PartitionOffset]]

    def purge: F[Option[PartitionOffset]]

    def size[A](implicit kafkaRead: KafkaRead[F, A], eventualRead: EventualRead[F, A]): F[Long]
  }

  object JournalTest {

    def apply[F[_]: Monad](
      journal: Journal[F],
      timestamp: Instant,
    ): JournalTest[F] = new JournalTest[F] {

      def append[A](events: Nel[Event[A]], metadata: RecordMetadata, headers: Headers)(implicit
        kafkaWrite: KafkaWrite[F, A],
      ) =
        journal.append(events, metadata, headers)

      def read[A](implicit kafkaRead: KafkaRead[F, A], eventualRead: EventualRead[F, A]) =
        for {
          records <- journal.read().toList
        } yield for {
          record <- records
        } yield record.copy(timestamp = timestamp)

      def pointer = journal.pointer

      def delete(to: DeleteTo) = journal.delete(to)

      def purge = journal.purge

      def size[A](implicit kafkaRead: KafkaRead[F, A], eventualRead: EventualRead[F, A]) = journal.read().length
    }
  }
}
