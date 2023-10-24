package com.evolutiongaming.kafka.journal


import cats.data.{NonEmptyList => Nel}
import cats.effect.IO
import cats.effect.syntax.resource._
import cats.syntax.all._
import com.evolutiongaming.catshelper.DataHelper._
import com.evolutiongaming.catshelper.ParallelHelper._
import com.evolutiongaming.catshelper.{Log, LogOf, MeasureDuration}
import com.evolutiongaming.kafka.journal.IOSuite._
import com.evolutiongaming.kafka.journal.TestJsonCodec.instance
import com.evolutiongaming.kafka.journal.eventual.EventualJournal
import com.evolutiongaming.kafka.journal.util.PureConfigHelper._
import org.scalatest.wordspec.AsyncWordSpec

import java.time.Instant
import java.time.temporal.ChronoUnit
import scala.concurrent.duration._

class JournalPerfSpec extends AsyncWordSpec with JournalSuite {
  import JournalSuite._

  private val many = 100
  private val events = 1000
  private val origin = Origin("JournalPerfSpec")
  private val timestamp = Instant.now().truncatedTo(ChronoUnit.MILLIS)

  import cats.effect.unsafe.implicits.global

  private val journalOf = {
    eventualJournal: EventualJournal[IO] => {
      implicit val logOf = LogOf.empty[IO]
      val log = Log.empty[IO]
      val headCacheOf = HeadCacheOf[IO](HeadCacheMetrics.empty[IO].some)
      for {
        config    <- config.liftTo[IO].toResource
        consumer   = Journals.Consumer.of[IO](config.journal.kafka.consumer, config.journal.pollTimeout)
        headCache <- headCacheOf(config.journal.kafka.consumer, eventualJournal)
      } yield {
        Journals(
          producer = producer,
          origin = origin.some,
          consumer = consumer,
          eventualJournal = eventualJournal,
          headCache = headCache,
          log = log,
          conversionMetrics = none
        )
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

    def append(journals: Journals[IO]) = {

      val journal = JournalTest(journals(key), timestamp)

      val expected = {
        val expected = for {
          n     <- (0 until events).toList
          seqNr <- SeqNr.min.map[Option](_ + n)
        } yield {
          event(seqNr)
        }
        Nel.fromListUnsafe(expected)
      }

      def appendNoise = {
        (1 to events)
          .toList
          .parFoldMap1 { n =>
            val e = event(SeqNr.unsafe(events + n))
            for {
              _       <- journal.append(Nel.of(e))
              key     <- Key.random[IO]("journal")
              journal  = JournalTest(journals(key), timestamp)
              _       <- journal.append(Nel.of(e))
            } yield {}
          }
      }

      for {
        _ <- journal.pointer
        _ <- expected.groupedNel(10).foldMap { events => journal.append(events).void }
        _ <- appendNoise
      } yield {}
    }

    val appendToJournal = for {
      _ <- awaitResources
      _ <- journalOf(eventualJournal).use(append)
    } yield {}

    appendToJournal.start.void.unsafeRunSync()

    for {
      (eventualName, expected, eventual) <- List(
        ("empty"    , 2.second, () => EventualJournal.empty[IO]),
        ("non-empty", 1.second, () => eventualJournal))
    } {
      val name = s"events: $events, eventual: $eventualName"

      lazy val (journal, release) = {
        val (journals, release) = journalOf(eventual()).allocated.unsafeRunSync()
        (JournalTest(journals(key), timestamp), release)
      }

      s"measure pointer $many times, $name" in {
        val result = for {
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

  private def event(seqNr: SeqNr) =
    Event[Payload](seqNr)
}
