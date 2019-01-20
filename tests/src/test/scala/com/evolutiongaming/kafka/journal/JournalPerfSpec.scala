package com.evolutiongaming.kafka.journal

import java.util.UUID

import cats.implicits._
import cats.effect.{Clock, IO}
import com.evolutiongaming.kafka.journal.eventual.EventualJournal
import com.evolutiongaming.kafka.journal.util.IOSuite._
import com.evolutiongaming.kafka.journal.util.ClockHelper._
import com.evolutiongaming.nel.Nel
import org.scalatest.AsyncWordSpec

import scala.concurrent.duration._

class JournalPerfSpec extends AsyncWordSpec with JournalSuit {
  import JournalSuit._

  private val many = 100
  private val events = 1000

  private val origin = Origin("JournalPerfSpec")

  private val journalOf = {
    val consumer = Journal.Consumer.of[IO](config.journal.consumer)
    eventualJournal: EventualJournal[IO] => {
      implicit val log = Log.empty[IO]
      for {
        headCache <- HeadCache.of[IO](config.journal.consumer, eventualJournal)
        journal = Journal(
          kafkaProducer = producer,
          origin = Some(origin),
          consumer = consumer,
          eventualJournal = eventualJournal,
          pollTimeout = config.journal.pollTimeout,
          headCache = headCache)
      } yield journal
    }
  }

  def measure[A](fa: IO[A]): IO[FiniteDuration] = {
    for {
      durations <- (0 to many).foldLeft(List.empty[Long].pure[IO]) { (durations, _) =>
        for {
          durations <- durations
          start     <- Clock[IO].millis
          _         <- fa
          end       <- Clock[IO].millis
        } yield {
          val duration = end - start
          duration :: durations
        }
      }
    } yield {
      (durations.sum / durations.size).millis
    }
  }

  "Journal" should {

    val key = Key(id = UUID.randomUUID().toString, topic = "journal")

    lazy val append = {

      def append(journal0: Journal[IO]) = {

        val journal = KeyJournal(key, journal0)

        val expected = for {
          n <- (0 to events).toList
          seqNr <- SeqNr.Min.map(_ + n)
        } yield Event(seqNr)

        for {
          _ <- journal.pointer()
          _ <- expected.foldMap { event => journal.append(Nel(event)).void }
          _ <- {
            val otherEvents = for {_ <- 0 to events} yield Event(SeqNr.Min)
            otherEvents.toList.foldMap { event =>
              for {
                _       <- journal.append(Nel(event))
                key      = Key(id = UUID.randomUUID().toString, topic = "journal")
                journal  = KeyJournal(key, journal0)
                _       <- journal.append(Nel(event))
              } yield {}
            }
          }
        } yield {}
      }

      journalOf(eventual).use(append)
    }

    for {
      (eventualName, expected, eventual) <- List(
        ("empty", 2.second, () => EventualJournal.empty[IO]),
        ("non-empty", 1.second, () => eventual))
    } {
      val name = s"events: $events, eventual: $eventualName"

      lazy val (journal, release) = {
        val (journal, release) = journalOf(eventual()).allocated.unsafeRunSync()
        (KeyJournal(key, journal), release)
      }

      s"measure pointer $many times, $name" in {
        val result = for {
          _       <- append
          _       <- journal.pointer()
          average <- measure { journal.pointer() }
        } yield {
          info(s"pointer measured $many times for $events events returned on average in $average")
          average should be <= expected
        }

        result.run(5.minutes)
      }

      s"measure read $many times, $name" in {
        val result = for {
          _       <- journal.size()
          average <- measure { journal.size() }
          _       <- release
        } yield {
          info(s"read measured $many times for $events events returned on average in $average")
          average should be <= expected
        }

        result.run(5.minutes)
      }
    }
  }
}