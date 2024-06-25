package com.evolutiongaming.kafka.journal

import akka.persistence.kafka.journal.KafkaJournalConfig
import cats.Foldable
import cats.data.{NonEmptyList => Nel}
import cats.effect.{IO, Resource}
import cats.syntax.all._
import com.evolutiongaming.catshelper.ParallelHelper._
import com.evolutiongaming.catshelper.{Log, LogOf}
import com.evolutiongaming.kafka.journal.ExpireAfter.implicits._
import com.evolutiongaming.kafka.journal.IOSuite._
import com.evolutiongaming.kafka.journal.conversions.{KafkaRead, KafkaWrite}
import com.evolutiongaming.kafka.journal.eventual.{EventualJournal, EventualRead}
import com.evolutiongaming.kafka.journal.util.PureConfigHelper._
import com.evolutiongaming.retry.{Retry, Strategy}
import org.scalatest.Succeeded
import org.scalatest.wordspec.AsyncWordSpec
import play.api.libs.json.Json

import java.time.Instant
import java.time.temporal.ChronoUnit
import scala.concurrent.duration._

import TestJsonCodec.instance

abstract class JournalIntSpec[A] extends AsyncWordSpec with JournalSuite {
  import JournalIntSpec._
  import JournalSuite._

  def event(seqNr: SeqNr): Event[A]

  implicit val kafkaRead: KafkaRead[IO, A]
  implicit val kafkaWrite: KafkaWrite[IO, A]
  implicit val eventualRead: EventualRead[IO, A]

  import cats.effect.unsafe.implicits.global

  private val journalsOf = { (eventualJournal: EventualJournal[IO], headCache: Boolean) =>
    implicit val logOf = LogOf.empty[IO]
    val log            = Log.empty[IO]

    def headCacheOf(config: KafkaJournalConfig) = if (headCache) {
      val headCacheOf = HeadCacheOf[IO](HeadCacheMetrics.empty[IO].some)
      headCacheOf(config.journal.kafka.consumer, eventualJournal)
    } else {
      Resource.pure[IO, HeadCache[IO]](HeadCache.empty[IO])
    }

    for {
      config    <- config.liftTo[IO].toResource
      consumer   = Journals.Consumer.of[IO](config.journal.kafka.consumer, config.journal.pollTimeout)
      headCache <- headCacheOf(config)
      journal = Journals[IO](
        producer = producer,
        origin = origin.some,
        consumer = consumer,
        eventualJournal = eventualJournal,
        headCache = headCache,
        log = log,
        conversionMetrics = none,
      )
    } yield journal
  }

  "Journal" should {

    for {
      headCache <- List(true, false)
      (eventualName, eventualJournal) <- List(
        ("empty", () => EventualJournal.empty[IO]),
        ("non-empty", () => eventualJournal),
      )
    } {

      val name = s"eventual: $eventualName, headCache: $headCache"

      val key = Key.random[IO]("journal")

      lazy val (journals, release) = journalsOf(eventualJournal(), headCache).allocated
        .unsafeRunSync()

      for {
        seqNr <- List(SeqNr.min, SeqNr.unsafe(2))
      } {

        val name1 = s"seqNr: $seqNr, $name"

        s"append, delete, read, purge, lastSeqNr, $name1" in {
          val result = for {
            key      <- key
            journal   = JournalTest(journals(key), timestamp)
            pointer  <- journal.pointer
            _         = pointer shouldEqual None
            events   <- journal.read
            _         = events shouldEqual List.empty
            pointer  <- journal.delete(DeleteTo.max)
            _         = pointer shouldEqual None
            anEvent   = event(seqNr)
            offset   <- journal.append(Nel.of(anEvent), recordMetadata, headers)
            record    = EventRecord(anEvent, timestamp, offset, origin.some, version.some, recordMetadata, headers)
            partition = offset.partition
            events   <- journal.read
            _         = events shouldEqual List(record)
            pointer  <- journal.delete(DeleteTo.max)
            _         = pointer.map(_.partition) shouldEqual partition.some
            pointer  <- journal.pointer
            _         = pointer shouldEqual seqNr.some
            events   <- journal.read
            _         = events shouldEqual List.empty
            pointer  <- journal.purge
            _         = pointer.map(_.partition) shouldEqual partition.some
            pointer  <- journal.pointer
            _         = pointer shouldEqual none
            events   <- journal.read
            _         = events shouldEqual List.empty
            metadata  = recordMetadata.withExpireAfter(1.day.toExpireAfter.some)
            offset   <- journal.append(Nel.of(anEvent), metadata, headers)
            record    = EventRecord(anEvent, timestamp, offset, origin.some, version.some, metadata, headers)
            events   <- journal.read
            _         = events shouldEqual List(record)
            pointer  <- journal.delete(DeleteTo.max)
            _         = pointer.map(_.partition) shouldEqual partition.some
            pointer  <- journal.pointer
            _         = pointer shouldEqual seqNr.some
            pointer  <- journal.purge
            _         = pointer.map(_.partition) shouldEqual partition.some
            pointer  <- journal.pointer
            _         = pointer shouldEqual none
          } yield Succeeded

          result.run(1.minute)
        }

        val many = 10
        s"append & read $many, $name1" in {

          val events = for {
            n     <- (0 until many).toList
            seqNr <- seqNr.map[Option](_ + n)
          } yield event(seqNr)

          val result = for {
            key    <- key
            journal = JournalTest(journals(key), timestamp)
            read = for {
              events1 <- journal.read
              _        = events1.map(_.event) shouldEqual events
              pointer <- journal.pointer
              _        = pointer shouldEqual events.lastOption.map(_.seqNr)
            } yield {}
            _    <- journal.append(Nel.fromListUnsafe(events))
            reads = List.fill(10)(read)
            _    <- Foldable[List].fold(reads)
          } yield {}

          result.run(1.minute)
        }

        s"append & read $many in parallel, $name1" in {

          val expected = for {
            n     <- (0 to 10).toList
            seqNr <- seqNr.map[Option](_ + n)
          } yield event(seqNr)

          val appends = for {
            key     <- key
            journal  = JournalTest(journals(key), timestamp)
            events  <- journal.read
            _        = events shouldEqual Nil
            pointer <- journal.pointer
            _        = pointer shouldEqual None
            _       <- expected.foldMap(event => journal.append(Nel.of(event)).void)
          } yield for {
            pointer <- journal.pointer
            _        = pointer shouldEqual expected.lastOption.map(_.seqNr)
            events  <- journal.read
            _        = events.map(_.event) shouldEqual expected
          } yield {}
          List
            .fill(10)(appends)
            .parSequence
            .flatMap(_.parFold1)
            .run(1.minute)
        }

        s"append duplicates $name1" ignore {

          val seqNrs = {
            val seqNrs = (0 to 2).foldLeft(Nel.of(seqNr)) { (seqNrs, _) =>
              seqNrs.head.next[Option].fold(seqNrs)(_ :: seqNrs)
            }
            seqNrs.reverse
          }

          val result = for {
            key     <- key
            journal  = JournalTest(journals(key), timestamp)
            pointer <- journal.pointer
            _        = pointer shouldEqual None
            events  <- journal.read
            _        = events shouldEqual Nil
            offset  <- journal.delete(DeleteTo.max)
            _        = offset shouldEqual None
            events   = seqNrs.map(seqNr => event(seqNr))
            append   = journal.append(events, recordMetadata, headers)
            _       <- append
            _       <- append
            offset  <- append
            records = events.map { event =>
              EventRecord(event, timestamp, offset, origin.some, version.some, recordMetadata, headers)
            }
            partition = offset.partition
            events   <- journal.read
            _         = events shouldEqual records.toList
            offset   <- journal.delete(seqNrs.last.toDeleteTo)
            _         = offset.map(_.partition) shouldEqual partition.some
            pointer  <- journal.pointer
            _         = pointer shouldEqual seqNrs.last.some
            events   <- journal.read
            _         = events shouldEqual Nil
          } yield Succeeded

          result.run(1.minute)
        }
      }

      if (headCache) {
        s"expire records $name" ignore {
          val result = for {
            key     <- key
            journal  = journals(key)
            metadata = RecordMetadata(payload = PayloadMetadata(1.second.toExpireAfter.some))
            _       <- journal.append(Nel.of(event(SeqNr.min)), metadata)
            events  <- journal.read().toList
            _        = events.map(_.seqNr) shouldEqual List(SeqNr.min)
            strategy = Strategy.const(100.millis).limit(10.seconds)
            retry    = Retry[IO, Throwable](strategy)
            _ <- retry {
              for {
                events <- journal.read().toList
                _       = events shouldEqual List.empty
              } yield {}
            }
          } yield {}

          result.run(1.minute)
        }
      }

      s"release $name" in {
        release.run(1.minute)
      }
    }

    s"ids" in {
      val result = for {
        ids    <- eventualJournal.ids("journal").toList
        _      <- IO(ids should not be empty)
        result <- IO(ids.distinct shouldEqual ids)
      } yield result
      result.run(1.minute)
    }
  }
}

object JournalIntSpec {
  private val timestamp      = Instant.now().truncatedTo(ChronoUnit.MILLIS)
  private val origin         = Origin("JournalIntSpec")
  private val version        = Version.current
  private val recordMetadata = RecordMetadata(HeaderMetadata(Json.obj(("key", "value")).some), PayloadMetadata.empty)
  private val headers        = Headers(("key", "value"))

  implicit class EventRecordOps[A](val self: EventRecord[A]) extends AnyVal {
    def fix: EventRecord[A] = self.copy(timestamp = timestamp)
  }
}
