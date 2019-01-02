package com.evolutiongaming.kafka.journal

import java.util.UUID

import com.evolutiongaming.concurrent.async.Async
import com.evolutiongaming.kafka.journal.eventual.EventualJournal
import com.evolutiongaming.kafka.journal.AsyncHelper._
import com.evolutiongaming.kafka.journal.util.IOSuite._
import com.evolutiongaming.nel.Nel
import org.scalatest.{AsyncWordSpec, Succeeded}

import scala.compat.Platform
import scala.concurrent.duration._

class JournalPerfSpec extends AsyncWordSpec with JournalSuit {
  import JournalSuit._

  private val many = 100
  private val events = 1000

  private val origin = Origin("JournalPerfSpec")

  private lazy val journalOf = {
    val topicConsumer = TopicConsumer(config.journal.consumer, ecBlocking)
    eventual: EventualJournal[Async] => {
      val headCache = HeadCacheAsync(config.journal.consumer, eventual, ecBlocking)
      val journal = Journal(
        producer = producer,
        origin = Some(origin),
        topicConsumer = topicConsumer,
        eventual = eventual,
        pollTimeout = config.journal.pollTimeout,
        closeTimeout = config.journal.closeTimeout,
        headCache = headCache)
      (journal, () => headCache.close)
    }
  }

  def measure[A](f: => Async[A]): Async[FiniteDuration] = {
    for {
      durations <- (0 to many).foldLeft(Async(List.empty[Long])) { (durations, _) =>
        for {
          durations <- durations
          start = Platform.currentTime
          _ <- f
        } yield {
          val duration = Platform.currentTime - start
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
      val (journal0, release) = journalOf(eventual)
      val journal = KeyJournal(key, journal0)

      val expected = for {
        n <- (0 to events).toList
        seqNr <- SeqNr.Min.map(_ + n)
      } yield Event(seqNr)

      for {
        _ <- journal.pointer()
        _ <- expected.foldLeft(Async.unit) { (async, event) =>
          for {
            _ <- async
            _ <- journal.append(Nel(event))
          } yield {}
        }

        _ <- {
          val otherEvents = for {
            _ <- 0 to events
          } yield {
            Event(SeqNr.Min)
          }
          otherEvents.foldLeft(Async.unit) { (async, event) =>
            for {
              _ <- async
              _ <- journal.append(Nel(event))
              key = Key(id = UUID.randomUUID().toString, topic = "journal")
              journal = KeyJournal(key, journal0)
              _ <- journal.append(Nel(event))
            } yield {}
          }
        }
        _ <- release()
      } yield {}
    }

    for {
      (eventualName, expected, eventual) <- List(
        ("empty", 2.second, () => EventualJournal.empty[Async]),
        ("non-empty", 1.second, () => eventual))
    } {
      val name = s"events: $events, eventual: $eventualName"

      lazy val (journal, release) = {
        val (journal, release) = journalOf(eventual())
        (KeyJournal(key, journal), release)
      }

      s"measure pointer $many times, $name" in {
        val result = for {
          _ <- append
          _ <- journal.pointer()
          average <- measure {
            journal.pointer()
          }
        } yield {
          info(s"pointer measured $many times for $events events returned on average in $average")
          average should be <= expected
          Succeeded
        }

        result.future
      }

      s"measure read $many times, $name" in {
        val result = for {
          _ <- append
          _ <- journal.size()
          average <- measure {
            journal.size()
          }
          _ <- release()
        } yield {
          info(s"read measured $many times for $events events returned on average in $average")
          average should be <= expected
          Succeeded
        }

        result.future
      }
    }
  }
}