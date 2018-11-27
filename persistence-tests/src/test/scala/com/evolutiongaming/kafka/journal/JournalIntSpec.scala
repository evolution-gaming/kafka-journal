package com.evolutiongaming.kafka.journal

import java.util.UUID

import com.evolutiongaming.concurrent.async.Async
import com.evolutiongaming.kafka.journal.AsyncHelper._
import com.evolutiongaming.kafka.journal.eventual.EventualJournal
import com.evolutiongaming.nel.Nel

import scala.concurrent.duration._

class JournalIntSpec extends JournalSuit {
  import JournalSuit._

  private val timeout = 30.seconds

  val origin = Origin("JournalIntSpec")

  private lazy val journalOf = {
    val topicConsumer = TopicConsumer(config.journal.consumer, ecBlocking)
    (eventual: EventualJournal, key: Key) => {
      val journal = Journal(
        producer = producer,
        origin = Some(origin),
        topicConsumer = topicConsumer,
        eventual = eventual,
        pollTimeout = config.journal.pollTimeout,
        closeTimeout = config.journal.closeTimeout)
      KeyJournal(key, journal, timeout)
    }
  }

  "Journal" should {

    for {
      seqNr <- List(SeqNr.Min, SeqNr(10))
      (eventualName, eventual) <- List(
        ("empty", () => EventualJournal.Empty),
        ("non-empty", () => eventual))
    } {
      val name = s"seqNr: $seqNr, eventual: $eventualName"

      def keyOf() = Key(id = UUID.randomUUID().toString, topic = "journal")

      s"append, delete, read, lastSeqNr, $name" in {
        val key = keyOf()
        val journal = journalOf(eventual(), key)
        journal.pointer() shouldEqual None
        journal.read() shouldEqual Nil
        journal.delete(SeqNr.Max) shouldEqual None
        val event = Event(seqNr)
        val partition = journal.append(Nel(event)).partition
        journal.read() shouldEqual List(event)
        journal.delete(SeqNr.Max).map(_.partition) shouldEqual Some(partition)
        journal.pointer() shouldEqual Some(seqNr)
        journal.read() shouldEqual Nil
      }

      val many = 100
      s"append & read $many, $name" in {
        val key = keyOf()
        val journal = journalOf(eventual(), key)

        val events = for {
          n <- 0 until many
          seqNr <- seqNr.map(_ + n)
        } yield {
          Event(seqNr)
        }

        journal.append(Nel.unsafe(events))

        Async.foldUnit {
          for {
            _ <- 0 to 10
          } yield Async.async {
            journal.read() shouldEqual events
            journal.pointer() shouldEqual events.lastOption.map(_.seqNr)
          }
        }.get(timeout)
      }

      s"read $many in parallel, $name" in {

        val events = for {
          n <- (0 to 10).toList
          seqNr <- seqNr.map(_ + n)
        } yield Event(seqNr)

        val results = for {
          _ <- 0 until many
        } yield Async.async {
          val key = keyOf()
          val journal = journalOf(eventual(), key)
          journal.read() shouldEqual Nil
          journal.pointer() shouldEqual None
          for {
            event <- events
          } yield {
            journal.append(Nel(event))
          }
          () =>
            Async.async {
              journal.pointer() shouldEqual events.lastOption.map(_.seqNr)
              journal.read() shouldEqual events
              ()
            }
        }

        val recoveries = Async.list(results.toList).get(timeout)

        val recovered = for {
          recovery <- recoveries
        } yield {
          recovery()
        }

        Async.foldUnit(recovered).get(timeout)
      }
    }
  }
}