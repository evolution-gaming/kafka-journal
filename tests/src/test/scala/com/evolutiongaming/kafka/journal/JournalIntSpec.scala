package com.evolutiongaming.kafka.journal


import java.time.Instant
import java.time.temporal.ChronoUnit

import cats.Foldable
import cats.effect.{IO, Resource}
import cats.implicits._
import com.evolutiongaming.kafka.journal.IOSuite._
import com.evolutiongaming.kafka.journal.eventual.EventualJournal
import com.evolutiongaming.kafka.journal.CatsHelper._
import com.evolutiongaming.nel.Nel
import org.scalatest.{AsyncWordSpec, Succeeded}
import play.api.libs.json.Json

import scala.concurrent.duration._

class JournalIntSpec extends AsyncWordSpec with JournalSuite {
  import JournalSuite._
  import JournalIntSpec._

  private val journalOf = {

    val consumer = Journal.Consumer.of[IO](config.journal.consumer, config.journal.pollTimeout)

    (eventualJournal: EventualJournal[IO], headCache: Boolean) => {
      implicit val log = Log.empty[IO]
      implicit val logOf = LogOf.empty[IO]

      val headCache1 = if (headCache) {
        HeadCache.of[IO](
          config.journal.consumer,
          eventualJournal,
          HeadCache.Metrics.empty[IO].some)
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
          headCache = headCache)
      } yield journal
    }
  }

  "Journal" should {

    for {
      headCache                <- List(true, false)
      (eventualName, eventual) <- List(
        ("empty",     () => EventualJournal.empty[IO]),
        ("non-empty", () => eventual))
    } {

      val name = s"eventual: $eventualName, headCache: $headCache"

      val key = Key.random[IO]("journal")

      lazy val (journal0, release) = journalOf(eventual(), headCache).allocated.unsafeRunSync()

      for {
        seqNr <- List(SeqNr.Min, SeqNr(2))
      } {

        val name1 = s"seqNr: $seqNr, $name"

        s"append, delete, read, lastSeqNr, $name1" in {
          val result = for {
            key       <- key
            journal    = KeyJournal(key, timestamp, journal0)
            pointer   <- journal.pointer
            _          = pointer shouldEqual None
            events    <- journal.read
            _          = events shouldEqual Nil
            offset    <- journal.delete(SeqNr.Max)
            _          = offset shouldEqual None
            event      = Event(seqNr)
            offset    <- journal.append(Nel(event), metadata.data, headers)
            record     = EventRecord(event, timestamp, offset, origin.some, metadata, headers)
            partition  = offset.partition
            events    <- journal.read
            _          = events shouldEqual List(record)
            offset    <- journal.delete(SeqNr.Max)
            _          = offset.map(_.partition) shouldEqual Some(partition)
            pointer   <- journal.pointer
            _          = pointer shouldEqual Some(seqNr)
            events    <- journal.read
            _          = events shouldEqual Nil
          } yield Succeeded

          result.run(1.minute)
        }

        val many = 10
        s"append & read $many, $name1" in {

          val events = for {
            n <- 0 until many
            seqNr <- seqNr.map(_ + n)
          } yield {
            Event(seqNr)
          }

          val result = for {
            key     <- key
            journal  = KeyJournal(key, timestamp, journal0)
            read = for {
              events1 <- journal.read
              _        = events1.map(_.event) shouldEqual events
              pointer <- journal.pointer
              _ = pointer shouldEqual events.lastOption.map(_.seqNr)
            } yield {}
            _       <- journal.append(Nel.unsafe(events))
            reads    = List.fill(10)(read)
            _       <- Foldable[List].fold(reads)
          } yield {}

          result.run(1.minute)
        }

        s"append & read $many in parallel, $name1" in {

          val expected = for {
            n <- (0 to 10).toList
            seqNr <- seqNr.map(_ + n)
          } yield Event(seqNr)

          val appends = for {
            key     <- key
            journal  = KeyJournal(key, timestamp, journal0)
            events  <- journal.read
            _        = events shouldEqual Nil
            pointer <- journal.pointer
            _        = pointer shouldEqual None
            _       <- expected.foldMap { event => journal.append(Nel(event)).void }
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
            val seqNrs = (0 to 2).foldLeft(Nel(seqNr)) { (seqNrs, _) =>
              seqNrs.head.next.fold(seqNrs) { _ :: seqNrs }
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
            offset    <- journal.delete(SeqNr.Max)
            _          = offset shouldEqual None
            events     = seqNrs.map { seqNr => Event(seqNr) }
            append     = journal.append(events, metadata.data, headers)
            _         <- append
            _         <- append
            offset    <- append
            records    = events.map { event => EventRecord(event, timestamp, offset, origin.some, metadata, headers) }
            partition  = offset.partition
            events    <- journal.read
            _          = events shouldEqual records.toList
            offset    <- journal.delete(seqNrs.last)
            _          = offset.map(_.partition) shouldEqual Some(partition)
            pointer   <- journal.pointer
            _          = pointer shouldEqual Some(seqNrs.last)
            events    <- journal.read
            _          = events shouldEqual Nil
          } yield Succeeded

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
  private val metadata = Metadata(data = Some(Json.obj(("key", "value"))))
  private val headers = Headers(("key", "value"))

  implicit class EventRecordOps(val self: EventRecord) extends AnyVal {
    def fix: EventRecord = self.copy(timestamp = timestamp)
  }
}