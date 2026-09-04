package com.evolution.kafka.journal.it

import cats.data.NonEmptyList as Nel
import cats.effect.*
import cats.effect.implicits.*
import cats.implicits.*
import com.evolution.kafka.journal.*
import com.evolution.kafka.journal.IOSuite.*
import com.evolution.kafka.journal.eventual.EventualJournal
import com.evolutiongaming.catshelper.DataHelper.*
import com.evolutiongaming.catshelper.ParallelHelper.*
import com.evolutiongaming.catshelper.{Log, LogOf, MeasureDuration}
import org.scalatest.wordspec.AsyncWordSpec

import scala.concurrent.duration.*

class JournalPerfSpec extends AsyncWordSpec with JournalSuite {
  import IntegrationTestInstances.*
  import JournalSuite.*

  private val many = 100
  private val events = 1000
  private val origin = Origin("JournalPerfSpec")

  private def makeJournalTest(journal: Journal[IO]): JournalTest =
    new JournalTest(journal, readRecordTimestampOverride = None)

  private val journalOf = { (eventualJournal: EventualJournal[IO]) =>
    {
      implicit val logOf: LogOf[IO] = LogOf.empty
      val log = Log.empty[IO]
      val journalConfig = kafkaJournalConfig.journal
      val headCacheOf = HeadCacheOf[IO](metrics = none)
      val consumerResource = Journals.Consumer.make[IO](journalConfig.kafka.consumer, journalConfig.pollTimeout)

      for {
        headCache <- headCacheOf(journalConfig.kafka.consumer, eventualJournal)
      } yield {
        Journals(
          producer = producer,
          origin = origin.some,
          consumer = consumerResource,
          eventualJournal = eventualJournal,
          headCache = headCache,
          log = log,
          conversionMetrics = none,
        )
      }
    }
  }

  def measure[A](fa: IO[A]): IO[FiniteDuration] = {
    for {
      durations <- (0 to many).foldLeft(List.empty[Long].pure[IO]) { (durations, _) =>
        for {
          durations <- durations
          duration <- MeasureDuration[IO].start
          _ <- fa
          duration <- duration
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

      val journal = makeJournalTest(journals(key))

      val expected = {
        val expected = for {
          n <- (0 until events).toList
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
              _ <- journal.append(Nel.of(e))
              key <- Key.random[IO]("journal")
              journal = makeJournalTest(journals(key))
              _ <- journal.append(Nel.of(e))
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
        ("empty", 2.second, () => EventualJournal.empty[IO]),
        ("non-empty", 1.second, () => eventualJournal),
      )
    } {
      val name = s"events: $events, eventual: $eventualName"

      lazy val (journal, release) = {
        val (journals, release) = journalOf(eventual()).allocated.unsafeRunSync()
        (makeJournalTest(journals(key)), release)
      }

      s"measure pointer $many times, $name" in {
        val result = for {
          _ <- journal.pointer
          average <- measure { journal.pointer }
        } yield {
          info(s"pointer measured $many times for $events events returned on average in $average")
          average should be <= expected
        }

        result.run(5.minutes)
      }

      s"measure read $many times, $name" in {
        val result = for {
          _ <- journal.size
          average <- measure { journal.size }
          _ <- release
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
