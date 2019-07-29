package com.evolutiongaming.kafka.journal.replicator

import java.time.Instant

import cats.data.{NonEmptyList => Nel}
import cats.effect._
import cats.implicits._
import cats.temp.par.Par
import com.evolutiongaming.catshelper.{FromFuture, FromTry, LogOf, ToFuture, ToTry}
import com.evolutiongaming.kafka.journal.FixEquality.Implicits._
import com.evolutiongaming.kafka.journal._
import com.evolutiongaming.kafka.journal.eventual.EventualJournal
import com.evolutiongaming.kafka.journal.eventual.cassandra.{EventualCassandra, EventualCassandraConfig}
import com.evolutiongaming.kafka.journal.CassandraSuite._
import com.evolutiongaming.retry.Retry
import com.evolutiongaming.kafka.journal.IOSuite._
import com.evolutiongaming.kafka.journal.util.ActorSystemOf
import com.evolutiongaming.scassandra.CassandraClusterOf
import com.evolutiongaming.skafka.Offset
import com.evolutiongaming.smetrics.MeasureDuration
import com.typesafe.config.{Config, ConfigFactory}
import org.scalatest.{AsyncWordSpec, BeforeAndAfterAll, Matchers}
import play.api.libs.json.Json

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import scala.util.control.NoStackTrace

class ReplicatorIntSpec extends AsyncWordSpec with BeforeAndAfterAll with Matchers {

  private val origin = Origin.HostName getOrElse Origin("ReplicatorIntSpec")

  private val metadata = Metadata(data = Json.obj(("key", "value")).some)

  private val headers = Headers(("key", "value"))

  private implicit val randomId = RandomId.uuid[IO]

  private def resources[F[_] : Concurrent : LogOf : Par : FromFuture : Timer : ToFuture : ContextShift : RandomId : MeasureDuration : FromTry : ToTry](
    cassandraClusterOf: CassandraClusterOf[F]
  ) = {

    def eventualJournal(conf: Config) = {
      val config = Sync[F].delay { EventualCassandraConfig(conf.getConfig("cassandra")) }
      for {
        config          <- Resource.liftF[F, EventualCassandraConfig](config)
        eventualJournal <- EventualCassandra.of[F](config, None, cassandraClusterOf)
      } yield eventualJournal
    }

    def journal(
      conf: Config,
      blocking: ExecutionContext,
      eventualJournal: EventualJournal[F]
    ) = {

      val config = Sync[F].delay { JournalConfig(conf) }

      implicit val kafkaConsumerOf = KafkaConsumerOf[F](blocking)

      implicit val kafkaProducerOf = KafkaProducerOf[F](blocking)

      for {
        config   <- Resource.liftF(config)
        producer <- Journal.Producer.of[F](config.producer)
        consumer  = Journal.Consumer.of[F](config.consumer, config.pollTimeout)
        log      <- Resource.liftF(LogOf[F].apply(Journal.getClass))
      } yield {
        implicit val log1 = log
        Journal[F](
          origin = Some(origin),
          producer = producer,
          consumer = consumer,
          eventualJournal = eventualJournal,
          headCache = HeadCache.empty[F])
      }
    }

    val system = {
      val config = Sync[F].delay { ConfigFactory.load("replicator.conf") }
      for {
        config <- Resource.liftF(config)
        system <- ActorSystemOf[F](getClass.getSimpleName, Some(config))
      } yield system
    }

    for {
      system          <- system
      conf            <- Resource.liftF(Sync[F].delay { system.settings.config.getConfig("evolutiongaming.kafka-journal.replicator") })
      eventualJournal <- eventualJournal(conf)
      journal         <- journal(conf, system.dispatcher, eventualJournal)
    } yield {
      (eventualJournal, journal)
    }
  }

  lazy val ((eventualJournal, journal), release) = {
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

    implicit val fixEquality = FixEquality.array[Byte]()

    val topic = "journal"

    val strategy = Retry.Strategy.const(100.millis).limit(1.minute)

    val Error = new RuntimeException with NoStackTrace

    def read(key: Key)(until: List[EventRecord] => Boolean) = {
      val events = for {
        events <- eventualJournal.read(key, SeqNr.Min).toList
        events <- {
          if (until(events)) {
            val result = for {
              event <- events
            } yield {
              event.copy(timestamp = timestamp)
            }
            result.pure[IO]
          } else {
            Error.raiseError[IO, List[EventRecord]]
          }
        }
      } yield events

      Retry[IO, Throwable](strategy).apply(events)
    }

    def append(key: Key, events: Nel[Event]) = {
      for {
        partitionOffset <- journal.append(key, events, metadata.data, headers)
      } yield for {
        event <- events
      } yield {
        EventRecord(event, timestamp, partitionOffset, Some(origin), metadata, headers)
      }
    }

    def pointer(key: Key) = journal.pointer(key)

    def topicPointers = {
      for {
        pointers <- eventualJournal.pointers(topic)
      } yield {
        pointers.values
      }
    }

    for {
      seqNr <- List(1, 2, 10)
    } {

      s"replicate events and then delete, seqNr: $seqNr" in {

        val result = for {
          key             <- Key.random[IO](topic)
          pointer0        <- pointer(key)
          _                = pointer0 shouldEqual None
          pointers        <- topicPointers
          expected1       <- append(key, Nel.of(event(seqNr)))
          partitionOffset  = expected1.head.partitionOffset
          partition        = partitionOffset.partition
          _                = for {
            offset <- pointers.get(partitionOffset.partition)
          } partitionOffset.offset should be > offset
          events0         <- read(key)(_.nonEmpty)
          _                = events0 shouldEqual expected1.toList
          pointer1        <- pointer(key)
          _                = pointer1 shouldEqual Some(expected1.last.seqNr)
          pointer2        <- journal.delete(key, expected1.last.event.seqNr).map(_.map(_.partition))
          _                = pointer2 shouldEqual Some(partition)
          events1         <- read(key)(_.isEmpty)
          _                = events1 shouldEqual Nil
          pointer3        <- pointer(key)
          _                = pointer3 shouldEqual Some(expected1.last.seqNr)
          expected2       <- append(key, Nel.of(event(seqNr + 1), event(seqNr + 2)))
          events2         <- read(key)(_.nonEmpty)
          _                = events2 shouldEqual expected2.toList
          pointer4        <- pointer(key)
        } yield {
          pointer4 shouldEqual Some(expected2.last.seqNr)
        }

        result.run(5.minutes)
      }

      val numberOfEvents = 100

      s"replicate append of $numberOfEvents events, seqNr: $seqNr" in {


        val result = for {
          key        <- Key.random[IO](topic)
          events     = for {
            n <- (0 until numberOfEvents).toList
          } yield {
            event(seqNr + n, Payload("kafka-journal"))
          }
          expected   <- append(key, Nel.fromListUnsafe(events))
          actual     <- read(key)(_.nonEmpty)
          _           = actual.fix shouldEqual expected.toList.fix
          pointer    <- pointer(key)
          _           = pointer shouldEqual Some(events.last.seqNr)
        } yield {}

        result.run(5.minutes)
      }

      for {
        (name, events) <- List(
          ("empty", Nel.of(event(seqNr))),
          ("binary", Nel.of(event(seqNr, Payload.binary("binary")))),
          ("text", Nel.of(event(seqNr, Payload.text("text")))),
          ("json", Nel.of(event(seqNr, Payload.json("json")))),
          ("empty-many", Nel.of(
            event(seqNr),
            event(seqNr + 1),
            event(seqNr + 2))),
          ("binary-many", Nel.of(
            event(seqNr, Payload.binary("1")),
            event(seqNr + 1, Payload.binary("2")),
            event(seqNr + 2, Payload.binary("3")))),
          ("text-many", Nel.of(
            event(seqNr, Payload.text("1")),
            event(seqNr + 1, Payload.text("2")),
            event(seqNr + 2, Payload.text("3")))),
          ("json-many", Nel.of(
            event(seqNr, Payload.json("1")),
            event(seqNr + 1, Payload.json("2")),
            event(seqNr + 2, Payload.json("3")))),
          ("empty-binary-text-json", Nel.of(
            event(seqNr),
            event(seqNr + 1, Payload.binary("binary")),
            event(seqNr + 2, Payload.text("text")),
            event(seqNr + 3, Payload.json("json")))))
      } {
        s"consume event from kafka and replicate to eventual journal, seqNr: $seqNr, payload: $name" in {
          
          val result = for {
            key          <- Key.random[IO](topic)
            pointers     <- topicPointers
            expected     <- append(key, events)
            partition     = expected.head.partitionOffset.partition
            offsetBefore  = pointers.getOrElse(partition, Offset.Min)
            actual       <- read(key)(_.nonEmpty)
            _             = actual.fix shouldEqual expected.toList.fix
            pointer      <- pointer(key)
            _             = pointer shouldEqual Some(events.last.seqNr)
            pointers     <- topicPointers
            offsetAfter   = pointers.getOrElse(partition, Offset.Min)
          } yield {
            offsetAfter should be > offsetBefore
          }

          result.run(5.minutes)
        }
      }
    }
  }

  private def event(seqNr: Int, payload: Option[Payload] = None): Event = {
    val tags = (0 to seqNr).map(_.toString).toSet
    Event(SeqNr(seqNr.toLong), tags, payload)
  }

  private def event(seqNr: Int, payload: Payload): Event = {
    event(seqNr, Some(payload))
  }
}
