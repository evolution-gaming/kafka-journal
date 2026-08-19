package com.evolution.kafka.journal.it

import cats.data.NonEmptyList as Nel
import cats.effect.*
import cats.implicits.*
import com.evolution.kafka.journal.*
import com.evolution.kafka.journal.IOSuite.*
import com.evolution.kafka.journal.eventual.EventualJournal
import com.evolutiongaming.catshelper.DataHelper.*
import com.evolutiongaming.catshelper.MeasureDuration
import com.evolutiongaming.catshelper.ParallelHelper.*
import org.scalatest.freespec.AsyncFreeSpec

import scala.concurrent.duration.*

class JournalPerfSpec extends AsyncFreeSpec with JournalSuite {
  import ItInstances.*
  import JournalSuite.*

  private val many = 100
  private val events = 1000

  private val eventualCassandra = allocateSuiteScoped(eventualCassandraResource)
  private val journalProducer = allocateSuiteScoped(journalProducerResource)
  private val journalConsumerPool = allocateSuiteScoped(journalConsumerPoolResource)

  private val journalImplsToTest: Vector[TestJournalImpl] = Vector(
    new TestJournalImpl(
      eventualJournalImplName = "empty",
      eventualJournalImpl = EventualJournal.empty[IO],
      avgReadOpTimeAcceptanceThreshold = 2.seconds,
    ),
    new TestJournalImpl(
      eventualJournalImplName = "cassandra",
      eventualJournalImpl = eventualCassandra,
      avgReadOpTimeAcceptanceThreshold = 1.seconds,
    ),
  )

  private final class TestJournalImpl(
    val eventualJournalImplName: String,
    val eventualJournalImpl: EventualJournal[IO],
    val avgReadOpTimeAcceptanceThreshold: FiniteDuration,
  ) {
    val describe: String = s"[eventual=$eventualJournalImplName]"

    val journalsResource: ResourceIO[Journals[IO]] = makeJournalsResource(eventualJournalImpl)
  }

  private def makeJournalsResource(eventualJournal: EventualJournal[IO]): ResourceIO[Journals[IO]] = {
    for {
      headCache <- makeHeadCacheResource(eventualJournal, useHeadCache = true)
    } yield {
      makeJournals(
        producer = journalProducer,
        consumerResource = journalConsumerPool,
        eventualJournal = eventualJournal,
        headCache = headCache,
      )
    }
  }

  private def measure[A](fa: IO[A]): IO[FiniteDuration] = {
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

  "Journal performance" - {

    val key = Key.random[IO]("journal").unsafeRunSync()

    def append(journals: Journals[IO]) = {

      val journal = new JournalTest(journals(key))

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
              journal = new JournalTest(journals(key))
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

    val appendToJournal = makeJournalsResource(eventualCassandra).use(append)

    appendToJournal.unsafeRunSync()

    journalImplsToTest.foreach { testJournalImpl =>
      s"for impl ${ testJournalImpl.describe }" - {

        // msokolov:
        // Journal release logic here is broken, it was broken before my changes, and I'm not sure how to fix it.
        // release is called in the last test case, and it is not properly released if the last test case is
        // skipped.
        val (journals, release) = testJournalImpl.journalsResource.allocated.unsafeRunSync()
        val journal = new JournalTest(journals(key))

        s"measure pointer $many times" in {
          val result = for {
            _ <- journal.pointer
            average <- measure { journal.pointer }
          } yield {
            info(s"pointer measured $many times for $events events returned on average in $average")
            average should be <= testJournalImpl.avgReadOpTimeAcceptanceThreshold
          }

          result.run(5.minutes)
        }

        s"measure read $many times" in {
          val result = for {
            _ <- journal.size
            average <- measure { journal.size }
            _ <- release
          } yield {
            info(s"read measured $many times for $events events returned on average in $average")
            average should be <= testJournalImpl.avgReadOpTimeAcceptanceThreshold
          }

          result.run(5.minutes)
        }
      }
    }
  }

  private def event(seqNr: SeqNr) =
    Event[Payload](seqNr)
}
