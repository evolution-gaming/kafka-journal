package com.evolutiongaming.kafka.journal


import cats.Foldable
import cats.implicits._
import cats.effect.IO
import com.evolutiongaming.kafka.journal.eventual.EventualJournal
import com.evolutiongaming.kafka.journal.IOSuite._
import com.evolutiongaming.nel.Nel
import org.scalatest.{AsyncWordSpec, Succeeded}

import scala.concurrent.duration._

class JournalIntSpec extends AsyncWordSpec with JournalSuite {
  import JournalSuite._

  val origin = Origin("JournalIntSpec")

  private val journalOf = {
    val consumer = Journal.Consumer.of[IO](config.journal.consumer)
    eventualJournal: EventualJournal[IO] => {
      implicit val log = Log.empty[IO]
      implicit val logOf = LogOf.empty[IO]
      for {
        headCache <- HeadCache.of[IO](
          config.journal.consumer,
          eventualJournal,
          HeadCache.Metrics.empty[IO].some)
        journal = Journal[IO](
          producer = producer,
          origin = Some(origin),
          consumer = consumer,
          eventualJournal = eventualJournal,
          pollTimeout = config.journal.pollTimeout,
          headCache = headCache)
      } yield journal
    }
  }

  "Journal" should {

    for {
      seqNr                    <- List(SeqNr.Min, SeqNr(2))
      (eventualName, eventual) <- List(
        ("empty",     () => EventualJournal.empty[IO]),
        ("non-empty", () => eventual))
    } {
      val name = s"seqNr: $seqNr, eventual: $eventualName"

      val key = Key.random[IO]("journal")

      lazy val (journal0, release) = journalOf(eventual()).allocated.unsafeRunSync()

      s"append, delete, read, lastSeqNr, $name" in {
        val result = for {
          key       <- key
          journal    = KeyJournal(key, journal0)
          pointer   <- journal.pointer
          _          = pointer shouldEqual None
          events    <- journal.read
          _          = events shouldEqual Nil
          offset    <- journal.delete(SeqNr.Max)
          _          = offset shouldEqual None
          event      = Event(seqNr)
          offset    <- journal.append(Nel(event))
          partition  = offset.partition
          events    <- journal.read
          _          = events shouldEqual List(event)
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
      s"append & read $many, $name" in {

        val events = for {
          n <- 0 until many
          seqNr <- seqNr.map(_ + n)
        } yield {
          Event(seqNr)
        }

        val result = for {
          key     <- key
          journal  = KeyJournal(key, journal0)
          read = for {
            events  <- journal.read
            _        = events shouldEqual events
            pointer <- journal.pointer
            _ = pointer shouldEqual events.lastOption.map(_.seqNr)
          } yield {}
          _       <- journal.append(Nel.unsafe(events))
          reads    = List.fill(10)(read)
          _       <- Foldable[List].fold(reads)
        } yield {}

        result.run(1.minute)
      }

      s"append & read $many in parallel, $name" in {

        val expected = for {
          n <- (0 to 10).toList
          seqNr <- seqNr.map(_ + n)
        } yield Event(seqNr)

        val appends = for {
          key     <- key
          journal  = KeyJournal(key, journal0)
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
            _        = events shouldEqual expected
          } yield {}
        }

        val result = for {
          reads <- Par[IO].sequence(List.fill(10)(appends))
          _     <- Par[IO].fold(reads)
          _     <- release
        } yield {}

        result.run(1.minute)
      }
    }
  }
}