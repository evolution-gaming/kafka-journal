package com.evolutiongaming.kafka.journal

import java.util.UUID

import com.evolutiongaming.kafka.journal.eventual.EventualJournal
import com.evolutiongaming.nel.Nel
import com.evolutiongaming.safeakka.actor.ActorLog

import scala.compat.Platform
import scala.concurrent.duration._

class JournalPerfSpec extends JournalSuit {
  import JournalSuit._

  private val timeout = 5.minutes
  private val many = 100
  private val events = 1000

  private val origin = Origin("JournalPerfSpec")

  private lazy val journalOf = {
    val topicConsumer = TopicConsumer(config.journal.consumer, ecBlocking)
    (eventual: EventualJournal, key: Key) => {
      val log = ActorLog(system, HeadCache.getClass)
      val headCache = HeadCacheAsync(config.journal.consumer, eventual, ecBlocking, log)
      val journal = Journal(
        producer = producer,
        origin = Some(origin),
        topicConsumer = topicConsumer,
        eventual = eventual,
        pollTimeout = config.journal.pollTimeout,
        closeTimeout = config.journal.closeTimeout,
        readJournal = headCache)
      KeyJournal(key, journal, timeout)
    }
  }

  def measure[A](f: => A): FiniteDuration = {
    val durations = {
      val durations = for {
        _ <- (0 to many).toList
      } yield {
        val start = Platform.currentTime
        f
        val duration = Platform.currentTime - start
        duration
      }
      durations.tail
    }

    (durations.sum / durations.size).millis
  }

  "Journal" should {

    val key = Key(id = UUID.randomUUID().toString, topic = "journal")

    lazy val append: Unit = {
      val journal = journalOf(eventual, key)

      journal.pointer()

      for {
        n <- (0 to events).toList
        seqNr <- SeqNr.Min.map(_ + n)
      } {
        val event = Event(seqNr)
        journal.append(Nel(event))
      }

      for {
        _ <- 0 to events
        key = Key(id = UUID.randomUUID().toString, topic = "journal")
      } yield {
        val journal = journalOf(eventual, key)
        val event = Event(SeqNr.Min)
        journal.append(Nel(event))
      }
    }

    for {
      (eventualName, expected, eventual) <- List(
        ("empty", 2.second, () => EventualJournal.Empty),
        ("non-empty", 1.second, () => eventual))
    } {
      val name = s"events: $events, eventual: $eventualName"

      lazy val journal = journalOf(eventual(), key)

      s"measure pointer $many times, $name" in {
        append
        val average = measure {
          journal.pointer()
        }

        info(s"pointer measured $many times for $events events returned on average in $average")

        average should be <= expected
      }

      s"measure read $many times, $name" in {
        append
        val average = measure {
          journal.size()
        }

        info(s"read measured $many times for $events events returned on average in $average")

        average should be <= expected
      }
    }
  }
}