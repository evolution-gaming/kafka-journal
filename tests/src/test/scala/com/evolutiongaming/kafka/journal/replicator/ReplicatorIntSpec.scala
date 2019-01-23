package com.evolutiongaming.kafka.journal.replicator

import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.UUID

import cats.effect._
import cats.implicits._
import com.evolutiongaming.kafka.journal.FixEquality.Implicits._
import com.evolutiongaming.kafka.journal._
import com.evolutiongaming.kafka.journal.eventual.EventualJournal
import com.evolutiongaming.kafka.journal.eventual.cassandra.{EventualCassandra, EventualCassandraConfig}
import com.evolutiongaming.kafka.journal.retry.Retry
import com.evolutiongaming.kafka.journal.util.IOSuite._
import com.evolutiongaming.kafka.journal.util.{ActorSystemOf, FromFuture, Par, ToFuture}
import com.evolutiongaming.nel.Nel
import com.evolutiongaming.skafka.Offset
import com.typesafe.config.{Config, ConfigFactory}
import org.scalatest.{AsyncWordSpec, BeforeAndAfterAll, Matchers}

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import scala.util.control.NoStackTrace

class ReplicatorIntSpec extends AsyncWordSpec with BeforeAndAfterAll with Matchers {

  private val origin = Origin.HostName getOrElse Origin("ReplicatorIntSpec")

  private def resources[F[_] : Concurrent : LogOf : Par : FromFuture : Clock : ToFuture : ContextShift] = {

    def eventualJournal(conf: Config) = {
      val config = Sync[F].delay { EventualCassandraConfig(conf.getConfig("cassandra")) }
      for {
        config          <- Resource.liftF[F, EventualCassandraConfig](config)
        eventualJournal <- EventualCassandra.of[F](config, None)
      } yield eventualJournal
    }

    def journal(
      conf: Config,
      blocking: ExecutionContext,
      eventualJournal: EventualJournal[F]) = {

      val config = Sync[F].delay { JournalConfig(conf) }

      implicit val kafkaConsumerOf = KafkaConsumerOf[F](blocking)

      implicit val kafkaProducerOf = KafkaProducerOf[F](blocking)

      for {
        config   <- Resource.liftF(config)
        producer <- Journal.Producer.of[F](config.producer)
        consumer  = Journal.Consumer.of[F](config.consumer)
        log      <- Resource.liftF(LogOf[F].apply(Journal.getClass))
      } yield {
        implicit val log1 = log
        Journal[F](
          origin = Some(origin),
          producer = producer,
          consumer = consumer,
          eventualJournal = eventualJournal,
          pollTimeout = config.pollTimeout,
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

  lazy val ((eventualJournal, journal), release) = resources[IO].allocated.unsafeRunSync()

  override protected def beforeAll(): Unit = {
    IntegrationSuit.start()
    release
    super.beforeAll()
  }

  override protected def afterAll(): Unit = {
    release.unsafeRunSync()
    super.afterAll()
  }

  "Replicator" should {

    implicit val fixEquality = FixEquality.array[Byte]()

    val topic = "journal"

    val strategy = Retry.Strategy.const(100.millis).limit(1.minute)

    val Error = new RuntimeException with NoStackTrace

    def read(key: Key)(until: List[ReplicatedEvent] => Boolean) = {
      val events = for {
        events <- eventualJournal.read(key, SeqNr.Min).toList
        events <- if (until(events)) events.pure[IO] else Error.raiseError[IO, List[ReplicatedEvent]]
      } yield events

      Retry[IO, Throwable](strategy)((_, _) => ().pure[IO]).apply(events)
    }

    def append(key: Key, events: Nel[Event]) = {
      val timestamp = Instant.now().truncatedTo(ChronoUnit.MILLIS)
      for {
        partitionOffset <- journal.append(key, events, timestamp)
      } yield for {
        event <- events
      } yield {
        ReplicatedEvent(event, timestamp, partitionOffset, Some(origin))
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

        val key = Key(id = UUID.randomUUID().toString, topic = topic)

        val result = for {
          pointer0        <- pointer(key)
          _                = pointer0 shouldEqual None
          pointers        <- topicPointers
          expected1       <- append(key, Nel(event(seqNr)))
          partitionOffset  = expected1.head.partitionOffset
          partition        = partitionOffset.partition
          _                = for {
            offset <- pointers.get(partitionOffset.partition)
          } partitionOffset.offset should be > offset
          events0         <- read(key)(_.nonEmpty)
          _                = events0 shouldEqual expected1.toList
          pointer1        <- pointer(key)
          _                = pointer1 shouldEqual Some(expected1.last.seqNr)
          pointer2        <- journal.delete(key, expected1.last.event.seqNr, Instant.now()).map(_.map(_.partition))
          _                = pointer2 shouldEqual Some(partition)
          events1         <- read(key)(_.isEmpty)
          _                = events1 shouldEqual Nil
          pointer3        <- pointer(key)
          _                = pointer3 shouldEqual Some(expected1.last.seqNr)
          expected2       <- append(key, Nel(event(seqNr + 1), event(seqNr + 2)))
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
        val key = Key(id = UUID.randomUUID().toString, topic = topic)

        val events = for {
          n <- 0 until numberOfEvents
        } yield {
          event(seqNr + n, Payload("kafka-journal"))
        }

        val result = for {
          expected <- append(key, Nel.unsafe(events))
          actual   <- read(key)(_.nonEmpty)
          _         = actual.fix shouldEqual expected.toList.fix
          pointer  <- pointer(key)
          _         = pointer shouldEqual Some(events.last.seqNr)
        } yield {}

        result.run(5.minutes)
      }

      for {
        (name, events) <- List(
          ("empty", Nel(event(seqNr))),
          ("binary", Nel(event(seqNr, Payload.Binary("binary")))),
          ("text", Nel(event(seqNr, Payload.Text("text")))),
          ("json", Nel(event(seqNr, Payload.Json("json")))),
          ("empty-many", Nel(
            event(seqNr),
            event(seqNr + 1),
            event(seqNr + 2))),
          ("binary-many", Nel(
            event(seqNr, Payload.Binary("1")),
            event(seqNr + 1, Payload.Binary("2")),
            event(seqNr + 2, Payload.Binary("3")))),
          ("text-many", Nel(
            event(seqNr, Payload.Text("1")),
            event(seqNr + 1, Payload.Text("2")),
            event(seqNr + 2, Payload.Text("3")))),
          ("json-many", Nel(
            event(seqNr, Payload.Json("1")),
            event(seqNr + 1, Payload.Json("2")),
            event(seqNr + 2, Payload.Json("3")))),
          ("empty-binary-text-json", Nel(
            event(seqNr),
            event(seqNr + 1, Payload.Binary("binary")),
            event(seqNr + 2, Payload.Text("text")),
            event(seqNr + 3, Payload.Json("json")))))
      } {
        s"consume event from kafka and replicate to eventual journal, seqNr: $seqNr, payload: $name" in {

          val key = Key(id = UUID.randomUUID().toString, topic = topic)

          val result = for {
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
