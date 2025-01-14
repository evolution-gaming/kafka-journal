package com.evolutiongaming.kafka.journal.replicator

import cats.Parallel
import cats.data.NonEmptyList as Nel
import cats.effect.*
import cats.effect.syntax.resource.*
import cats.syntax.all.*
import com.evolutiongaming.catshelper.*
import com.evolutiongaming.kafka.journal.*
import com.evolutiongaming.kafka.journal.CassandraSuite.*
import com.evolutiongaming.kafka.journal.ExpireAfter.implicits.*
import com.evolutiongaming.kafka.journal.IOSuite.*
import com.evolutiongaming.kafka.journal.Journal.DataIntegrityConfig
import com.evolutiongaming.kafka.journal.eventual.cassandra.{EventualCassandra, EventualCassandraConfig}
import com.evolutiongaming.kafka.journal.eventual.{EventualJournal, EventualRead}
import com.evolutiongaming.kafka.journal.util.PureConfigHelper.*
import com.evolutiongaming.kafka.journal.util.{ActorSystemOf, Fail}
import com.evolutiongaming.retry.{Retry, Strategy}
import com.evolutiongaming.scassandra.CassandraClusterOf
import com.evolutiongaming.skafka.Offset
import com.typesafe.config.{Config, ConfigFactory}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec
import play.api.libs.json.Json
import pureconfig.ConfigSource

import java.time.Instant
import scala.concurrent.duration.*
import scala.util.control.NoStackTrace

import TestJsonCodec.instance

class ReplicatorIntSpec extends AsyncWordSpec with BeforeAndAfterAll with Matchers {

  import cats.effect.unsafe.implicits.global

  private val origin  = Origin("ReplicatorIntSpec")
  private val version = Version.current

  private val recordMetadata = RecordMetadata(HeaderMetadata(Json.obj(("key", "value")).some), PayloadMetadata.empty)

  private val headers = Headers(("key", "value"))

  private implicit val randomIdOf: RandomIdOf[IO] = RandomIdOf.uuid[IO]

  private def resources[F[_]: Async: LogOf: Parallel: FromFuture: ToFuture: RandomIdOf: MeasureDuration: FromTry: ToTry: Fail](
      cassandraClusterOf: CassandraClusterOf[F],
  ) = {

    def eventualJournal(conf: Config) = {
      val config = ConfigSource
        .fromConfig(conf)
        .at("cassandra")
        .load[EventualCassandraConfig]
        .liftTo[F]
      for {
        config          <- config.toResource
        eventualJournal <- EventualCassandra.make[F](config, origin.some, none, cassandraClusterOf, DataIntegrityConfig.Default)
      } yield eventualJournal
    }

    def journal(
        conf: Config,
        eventualJournal: EventualJournal[F],
    ) = {

      val config = ConfigSource
        .fromConfig(conf)
        .load[JournalConfig]
        .liftTo[F]

      implicit val kafkaConsumerOf = KafkaConsumerOf[F]()

      implicit val kafkaProducerOf = KafkaProducerOf[F]()

      for {
        config   <- config.toResource
        producer <- Journals.Producer.make[F](config.kafka.producer)
        consumer  = Journals.Consumer.make[F](config.kafka.consumer, config.pollTimeout)
        log      <- LogOf[F].apply(Journals.getClass).toResource
      } yield {
        Journals[F](
          origin            = origin.some,
          producer          = producer,
          consumer          = consumer,
          eventualJournal   = eventualJournal,
          headCache         = HeadCache.empty[F],
          log               = log,
          conversionMetrics = none,
        )
      }
    }

    val system = {
      val config = Sync[F].delay { ConfigFactory.load("replicator.conf") }
      for {
        config <- config.toResource
        system <- ActorSystemOf[F](getClass.getSimpleName, config.some)
      } yield system
    }

    for {
      system          <- system
      conf            <- Sync[F].delay { system.settings.config.getConfig("evolutiongaming.kafka-journal.replicator") }.toResource
      eventualJournal <- eventualJournal(conf)
      journal         <- journal(conf, eventualJournal)
    } yield {
      (eventualJournal, journal)
    }
  }

  lazy val ((eventualJournal, journals), release) = {
    implicit val logOf = LogOf.empty[IO]
    resources[IO](cassandraClusterOf).allocated.unsafeRunSync()
  }

  override protected def beforeAll(): Unit = {
    IntegrationSuite.start()
    release
    super.beforeAll()
  }

  override protected def afterAll(): Unit = {
    release.unsafeRunSync()
    super.afterAll()
  }

  "Replicator" should {

    val timestamp = Instant.now()

    val topic = "journal"

    val strategy = Strategy.const(100.millis).limit(1.minute)

    val Error = new RuntimeException with NoStackTrace

    val eventualRead = EventualRead.summon[IO, Payload]

    def read(key: Key)(until: List[EventRecord[Payload]] => Boolean) = {
      val events = for {
        events <- eventualJournal
          .read(key, SeqNr.min)
          .mapM(_.traverse(eventualRead.apply))
          .toList
        events <- {
          if (until(events)) {
            events
              .map { event => event.copy(timestamp = timestamp) }
              .pure[IO]
          } else {
            Error.raiseError[IO, List[EventRecord[Payload]]]
          }
        }
      } yield events

      Retry[IO, Throwable](strategy).apply(events)
    }

    def append(journal: Journal[IO], events: Nel[Event[Payload]], expireAfter: Option[ExpireAfter] = none) = {
      val recordMetadata1 = recordMetadata.withExpireAfter(expireAfter)
      for {
        partitionOffset <- journal.append(events, recordMetadata1, headers)
      } yield for {
        event <- events
      } yield {
        EventRecord(event, timestamp, partitionOffset, origin.some, version.some, recordMetadata1, headers)
      }
    }

    "replicate events and expire" in {
      val result = for {
        key       <- Key.random[IO](topic)
        journal    = journals(key)
        expected0 <- append(journal, Nel.of(event(1)))
        events    <- read(key)(_.nonEmpty)
        _          = events shouldEqual expected0.toList
        expected1 <- append(journal, Nel.of(event(2)), 1.day.toExpireAfter.some)
        events    <- read(key)(_.size == 2)
        _          = events shouldEqual expected0.toList ++ expected1.toList
        // TODO expiry: implement actual expiration test
      } yield {}
      result.run(5.minutes)
    }

    // TODO expiry: replicator should handle random message and not fail, headcache as well

    "replicate events and not expire" in {
      val result = for {
        key       <- Key.random[IO](topic)
        journal    = journals(key)
        expected0 <- append(journal, Nel.of(event(1)), 1.day.toExpireAfter.some)
        events    <- read(key)(_.nonEmpty)
        _          = events shouldEqual expected0.toList
        expected1 <- append(journal, Nel.of(event(2)))
        events    <- read(key)(_.size == 2)
        _          = events shouldEqual expected0.toList ++ expected1.toList
        // TODO expiry: how to verify
      } yield {}
      result.run(5.minutes)
    }

    "purge" in {
      val result = for {
        key      <- Key.random[IO](topic)
        journal   = journals(key)
        expected <- append(journal, Nel.of(event(1)))
        events   <- read(key)(_.nonEmpty)
        _         = events shouldEqual expected.toList
        pointer  <- journal.pointer
        _         = pointer shouldEqual expected.last.seqNr.some
        pointer  <- journal.purge
        _         = pointer.map { _.partition } shouldEqual expected.head.partition.some
        events   <- read(key)(_.isEmpty)
        _         = events shouldEqual Nil
        pointer  <- journal.pointer
        _         = pointer shouldEqual none
      } yield {}
      result.run(5.minutes)
    }

    for {
      seqNr <- List(1, 2, 10)
    } {

      s"replicate events and there after delete, seqNr: $seqNr" in {
        val result = for {
          key            <- Key.random[IO](topic)
          journal         = journals(key)
          pointer0       <- journal.pointer
          _               = pointer0 shouldEqual None
          expected       <- append(journal, Nel.of(event(seqNr)))
          partitionOffset = expected.head.partitionOffset
          partition       = partitionOffset.partition
          offset         <- eventualJournal.offset(topic, partitionOffset.partition)
          _               = offset.foreach { offset => partitionOffset.offset should be >= offset }
          events         <- read(key)(_.nonEmpty)
          _               = events shouldEqual expected.toList
          pointer        <- journal.pointer
          _               = pointer shouldEqual expected.last.seqNr.some
          pointer        <- journal.delete(expected.last.event.seqNr.toDeleteTo).map(_.map(_.partition))
          _               = pointer shouldEqual partition.some
          events         <- read(key)(_.isEmpty)
          _               = events shouldEqual Nil
          pointer        <- journal.pointer
          _               = pointer shouldEqual expected.last.seqNr.some
          expected       <- append(journal, Nel.of(event(seqNr + 1), event(seqNr + 2)))
          events         <- read(key)(_.nonEmpty)
          _               = events shouldEqual expected.toList
          pointer4       <- journal.pointer
          _               = pointer4 shouldEqual expected.last.seqNr.some
        } yield {}
        result.run(5.minutes)
      }

      val numberOfEvents = 100

      s"replicate append of $numberOfEvents events, seqNr: $seqNr" in {

        val result = for {
          key    <- Key.random[IO](topic)
          journal = journals(key)
          events = for {
            n <- (0 until numberOfEvents).toList
          } yield {
            event(seqNr + n, Payload("kafka-journal"))
          }
          expected <- append(journal, Nel.fromListUnsafe(events))
          actual   <- read(key)(_.nonEmpty)
          _         = actual shouldEqual expected.toList
          pointer  <- journal.pointer
          _         = pointer shouldEqual events.last.seqNr.some
        } yield {}

        result.run(5.minutes)
      }

      def binary(a: String) = PayloadBinaryFromStr(a)

      for {
        (name, events) <- List(
          ("empty", Nel.of(event(seqNr))),
          ("binary", Nel.of(event(seqNr, binary("binary")))),
          ("text", Nel.of(event(seqNr, Payload.text("text")))),
          ("json", Nel.of(event(seqNr, Payload.json("json")))),
          ("empty-many", Nel.of(event(seqNr), event(seqNr + 1), event(seqNr + 2))),
          ("binary-many", Nel.of(event(seqNr, binary("1")), event(seqNr + 1, binary("2")), event(seqNr + 2, binary("3")))),
          (
            "text-many",
            Nel.of(event(seqNr, Payload.text("1")), event(seqNr + 1, Payload.text("2")), event(seqNr + 2, Payload.text("3"))),
          ),
          (
            "json-many",
            Nel.of(event(seqNr, Payload.json("1")), event(seqNr + 1, Payload.json("2")), event(seqNr + 2, Payload.json("3"))),
          ),
          (
            "empty-binary-text-json",
            Nel.of(
              event(seqNr),
              event(seqNr + 1, binary("binary")),
              event(seqNr + 2, Payload.text("text")),
              event(seqNr + 3, Payload.json("json")),
            ),
          ),
        )
      } {
        s"consume event from kafka and replicate to eventual journal, seqNr: $seqNr, payload: $name" in {

          val result = for {
            key          <- Key.random[IO](topic)
            journal       = journals(key)
            expected     <- append(journal, events)
            partition     = expected.head.partitionOffset.partition
            offsetBefore <- eventualJournal.offset(topic, partition).map(_.getOrElse(Offset.min))
            actual       <- read(key)(_.nonEmpty)
            _             = actual shouldEqual expected.toList
            pointer      <- journal.pointer
            _             = pointer shouldEqual events.last.seqNr.some
            offsetAfter  <- eventualJournal.offset(topic, partition).map(_.getOrElse(Offset.min))
          } yield {
            offsetAfter should be > offsetBefore
          }

          result.run(5.minutes)
        }
      }
    }
  }

  private def event(seqNr: Int, payload: Option[Payload] = None): Event[Payload] = {
    val tags = (0 to seqNr).map(_.toString).toSet
    Event(SeqNr.unsafe(seqNr), tags, payload)
  }

  private def event(seqNr: Int, payload: Payload): Event[Payload] = {
    event(seqNr, payload.some)
  }
}
