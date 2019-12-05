package com.evolutiongaming.kafka.journal


import java.time.Instant
import java.time.temporal.ChronoUnit

import cats.data.{NonEmptyList => Nel}
import cats.implicits._
import cats.effect.IO
import com.evolutiongaming.kafka.journal.eventual.EventualJournal
import com.evolutiongaming.kafka.journal.IOSuite._
import com.evolutiongaming.kafka.journal.util.OptionHelper._
import com.evolutiongaming.catshelper.DataHelper._
import com.evolutiongaming.catshelper.ParallelHelper._
import com.evolutiongaming.catshelper.{Log, LogOf}
import com.evolutiongaming.smetrics.MeasureDuration
import org.scalatest.wordspec.AsyncWordSpec

import scala.concurrent.duration._

class JournalPerfSpec extends AsyncWordSpec with JournalSuite {
  import JournalSuite._

  private val many = 100
  private val events = 1000
  private val origin = Origin("JournalPerfSpec")
  private val timestamp = Instant.now().truncatedTo(ChronoUnit.MILLIS)

  private val journalOf = {
    val consumer = Journal.Consumer.of[IO](config.journal.consumer, config.journal.pollTimeout)
    eventualJournal: EventualJournal[IO] => {
      implicit val logOf = LogOf.empty[IO]
      val log = Log.empty[IO]
      val headCacheOf = HeadCacheOf[IO](HeadCacheMetrics.empty[IO].some)
      for {
        headCache <- headCacheOf(config.journal.consumer, eventualJournal)
      } yield {
        Journal(
          producer = producer,
          origin = Some(origin),
          consumer = consumer,
          eventualJournal = eventualJournal,
          headCache = headCache,
          log = log)
      }
    }
  }

  def measure[A](fa: IO[A]): IO[FiniteDuration] = {
    for {
      durations <- (0 to many).foldLeft(List.empty[Long].pure[IO]) { (durations, _) =>
        for {
          durations <- durations
          duration  <- MeasureDuration[IO].start
          _         <- fa
          duration  <- duration
        } yield {
          duration.toMillis :: durations
        }
      }
    } yield {
      (durations.sum / durations.size).millis
    }
  }

  "Journal" should {

    val key = Key.random[IO]("journal").unsafeRunSync()

    lazy val append = {

      def append(journal0: Journal[IO]) = {

        val journal = KeyJournal(key, timestamp, journal0)

        val expected = {
          val expected = for {
            n     <- (0 to events).toList
            seqNr <- SeqNr.min.map[Option](_ + n)
          } yield {
            Event(seqNr)
          }
          Nel.fromListUnsafe(expected)
        }

        def appendNoise = {
          (0 to events)
            .toList
            .parFoldMap { _ =>
              val event = Event(SeqNr.min)
              for {
                _       <- journal.append(Nel.of(event))
                key     <- Key.random[IO]("journal")
                journal  = KeyJournal(key, timestamp, journal0)
                _       <- journal.append(Nel.of(event))
              } yield {}
            }
        }

        for {
          _ <- journal.pointer
          _ <- expected.grouped(10).foldMap { events => journal.append(events).void }
          _ <- appendNoise
        } yield {}
      }

      journalOf(eventualJournal).use(append)
    }

    for {
      (eventualName, expected, eventual) <- List(
        ("empty"    , 2.second, () => EventualJournal.empty[IO]),
        ("non-empty", 1.second, () => eventualJournal))
    } {
      val name = s"events: $events, eventual: $eventualName"

      lazy val (journal, release) = {
        val (journal, release) = journalOf(eventual()).allocated.unsafeRunSync()
        (KeyJournal(key, timestamp, journal), release)
      }

      s"measure pointer $many times, $name" in {
        val result = for {
          _       <- append
          _       <- journal.pointer
          average <- measure { journal.pointer }
        } yield {
          info(s"pointer measured $many times for $events events returned on average in $average")
          average should be <= expected
        }

        result.run(5.minutes)
      }

      s"measure read $many times, $name" in {
        val result = for {
          _       <- journal.size
          average <- measure { journal.size }
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