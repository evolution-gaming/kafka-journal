package com.evolutiongaming.kafka.journal


import java.time.Instant
import java.time.temporal.ChronoUnit

import cats.Foldable
import cats.data.{NonEmptyList => Nel}
import cats.effect.{IO, Resource}
import cats.implicits._
import com.evolutiongaming.catshelper.ParallelHelper._
import com.evolutiongaming.catshelper.{Log, LogOf}
import com.evolutiongaming.kafka.journal.IOSuite._
import com.evolutiongaming.kafka.journal.eventual.EventualJournal
import com.evolutiongaming.kafka.journal.util.OptionHelper._
import com.evolutiongaming.kafka.journal.ExpireAfter.implicits._
import com.evolutiongaming.retry.{Retry, Strategy}
import org.scalatest.Succeeded
import org.scalatest.wordspec.AsyncWordSpec
import play.api.libs.json.Json

import scala.concurrent.duration._

class JournalIntSpec extends AsyncWordSpec with JournalSuite {
  import JournalIntSpec._
  import JournalSuite._

  private val journalOf = {

    val consumer = Journal.Consumer.of[IO](config.journal.consumer, config.journal.pollTimeout)

    (eventualJournal: EventualJournal[IO], headCache: Boolean) => {
      implicit val logOf = LogOf.empty[IO]
      val log = Log.empty[IO]

      val headCache1 = if (headCache) {
        val headCacheOf = HeadCacheOf[IO](HeadCacheMetrics.empty[IO].some)
        headCacheOf(
          config.journal.consumer,
          eventualJournal)
      } else {
        Resource.pure[IO, HeadCache[IO]](HeadCache.empty[IO])
      }

      for {
        headCache <- headCache1
        journal   = Journal[IO](
          producer = producer,
          origin = Some(origin),
          consumer = consumer,
          eventualJournal = eventualJournal,
          headCache = headCache,
          log = log)
      } yield journal
    }
  }

  "Journal" should {

    for {
      headCache                       <- List(true, false)
      (eventualName, eventualJournal) <- List(
        ("empty",     () => EventualJournal.empty[IO]),
        ("non-empty", () => eventualJournal))
    } {

      val name = s"eventual: $eventualName, headCache: $headCache"

      val key = Key.random[IO]("journal")

      lazy val (journal0, release) = journalOf(eventualJournal(), headCache).allocated.unsafeRunSync()

      for {
        seqNr <- List(SeqNr.min, SeqNr.unsafe(2))
      } {

        val name1 = s"seqNr: $seqNr, $name"

        s"append, delete, read, purge, lastSeqNr, $name1" in {
          val result = for {
            key       <- key
            journal    = KeyJournal(key, timestamp, journal0)
            pointer   <- journal.pointer
            _          = pointer shouldEqual None
            events    <- journal.read
            _          = events shouldEqual List.empty
            pointer   <- journal.delete(SeqNr.max)
            _          = pointer shouldEqual None
            event      = Event(seqNr)
            offset    <- journal.append(Nel.of(event), recordMetadata.data, headers)
            record     = EventRecord(event, timestamp, offset, origin.some, recordMetadata, headers)
            partition  = offset.partition
            events    <- journal.read
            _          = events shouldEqual List(record)
            pointer   <- journal.delete(SeqNr.max)
            _          = pointer.map { _.partition } shouldEqual partition.some
            pointer   <- journal.pointer
            _          = pointer shouldEqual seqNr.some
            events    <- journal.read
            _          = events shouldEqual List.empty
            pointer   <- journal.purge
            _          = pointer.map { _.partition } shouldEqual partition.some
            pointer   <- journal.pointer
            _          = pointer shouldEqual none
            events    <- journal.read
            _          = events shouldEqual List.empty
            offset    <- journal.append(Nel.of(event), recordMetadata.data, headers, 1.day.toExpireAfter.some)
            record     = EventRecord(event, timestamp, offset, origin.some, recordMetadata, headers)
            events    <- journal.read
            _          = events shouldEqual List(record)
            pointer   <- journal.delete(SeqNr.max)
            _          = pointer.map { _.partition } shouldEqual partition.some
            pointer   <- journal.pointer
            _          = pointer shouldEqual seqNr.some
            pointer   <- journal.purge
            _          = pointer.map { _.partition } shouldEqual partition.some
            pointer   <- journal.pointer
            _          = pointer shouldEqual none
          } yield Succeeded

          result.run(1.minute)
        }

        val many = 10
        s"append & read $many, $name1" in {

          val events = for {
            n     <- (0 until many).toList
            seqNr <- seqNr.map[Option](_ + n)
          } yield {
            Event(seqNr)
          }

          val result = for {
            key     <- key
            journal  = KeyJournal(key, timestamp, journal0)
            read     = for {
              events1 <- journal.read
              _        = events1.map(_.event) shouldEqual events
              pointer <- journal.pointer
              _        = pointer shouldEqual events.lastOption.map(_.seqNr)
            } yield {}
            _       <- journal.append(Nel.fromListUnsafe(events))
            reads    = List.fill(10)(read)
            _       <- Foldable[List].fold(reads)
          } yield {}

          result.run(1.minute)
        }

        s"append & read $many in parallel, $name1" in {

          val expected = for {
            n     <- (0 to 10).toList
            seqNr <- seqNr.map[Option](_ + n)
          } yield Event(seqNr)

          val appends = for {
            key     <- key
            journal  = KeyJournal(key, timestamp, journal0)
            events  <- journal.read
            _        = events shouldEqual Nil
            pointer <- journal.pointer
            _        = pointer shouldEqual None
            _       <- expected.foldMap { event => journal.append(Nel.of(event)).void }
          } yield {
            for {
              pointer <- journal.pointer
              _        = pointer shouldEqual expected.lastOption.map(_.seqNr)
              events  <- journal.read
              _        = events.map(_.event) shouldEqual expected
            } yield {}
          }

          val result = for {
            reads <- List.fill(10)(appends).parSequence
            _     <- reads.parFold
          } yield {}

          result.run(1.minute)
        }

        s"append duplicates $name1" ignore {

          val seqNrs = {
            val seqNrs = (0 to 2).foldLeft(Nel.of(seqNr)) { (seqNrs, _) =>
              seqNrs.head.next[Option].fold(seqNrs) { _ :: seqNrs }
            }
            seqNrs.reverse
          }

          val result = for {
            key       <- key
            journal    = KeyJournal(key, timestamp, journal0)
            pointer   <- journal.pointer
            _          = pointer shouldEqual None
            events    <- journal.read
            _          = events shouldEqual Nil
            offset    <- journal.delete(SeqNr.max)
            _          = offset shouldEqual None
            events     = seqNrs.map { seqNr => Event(seqNr) }
            append     = journal.append(events, recordMetadata.data, headers)
            _         <- append
            _         <- append
            offset    <- append
            records    = events.map { event => EventRecord(event, timestamp, offset, origin.some, recordMetadata, headers) }
            partition  = offset.partition
            events    <- journal.read
            _          = events shouldEqual records.toList
            offset    <- journal.delete(seqNrs.last)
            _          = offset.map(_.partition) shouldEqual partition.some
            pointer   <- journal.pointer
            _          = pointer shouldEqual seqNrs.last.some
            events    <- journal.read
            _          = events shouldEqual Nil
          } yield Succeeded

          result.run(1.minute)
        }
      }

      if (headCache) {
        s"expire records $name" ignore {
          val result = for {
            key      <- key
            _        <- journal0.append(key, Nel.of(Event(SeqNr.min)), 1.second.toExpireAfter.some)
            events   <- journal0.read(key).toList
            _         = events.map(_.seqNr) shouldEqual List(SeqNr.min)
            strategy  = Strategy.const(100.millis).limit(10.seconds)
            retry     = Retry(strategy)
            _        <- retry {
              for {
                events <- journal0.read(key).toList
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
  }
}

object JournalIntSpec {
  private val timestamp = Instant.now().truncatedTo(ChronoUnit.MILLIS)
  private val origin = Origin("JournalIntSpec")
  private val recordMetadata = RecordMetadata(data = Some(Json.obj(("key", "value"))))
  private val headers = Headers(("key", "value"))

  implicit class EventRecordOps(val self: EventRecord) extends AnyVal {
    def fix: EventRecord = self.copy(timestamp = timestamp)
  }
}