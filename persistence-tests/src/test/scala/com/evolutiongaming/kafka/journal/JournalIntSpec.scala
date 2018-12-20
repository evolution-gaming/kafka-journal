package com.evolutiongaming.kafka.journal

import java.util.UUID

import com.evolutiongaming.concurrent.async.Async
import com.evolutiongaming.kafka.journal.AsyncHelper._
import com.evolutiongaming.kafka.journal.AsyncImplicits._
import com.evolutiongaming.kafka.journal.eventual.EventualJournal
import com.evolutiongaming.nel.Nel
import com.evolutiongaming.safeakka.actor.ActorLog
import org.scalatest.{AsyncWordSpec, Succeeded}

class JournalIntSpec extends AsyncWordSpec with JournalSuit {
  import JournalSuit._

  val origin = Origin("JournalIntSpec")

  private lazy val journalOf = {
    val topicConsumer = TopicConsumer(config.journal.consumer, ecBlocking)
    eventual: EventualJournal[Async] => {
      val log = ActorLog(system, HeadCache.getClass)
      val headCache = HeadCacheAsync(config.journal.consumer, eventual, ecBlocking, log)
      Journal(
        producer = producer,
        origin = Some(origin),
        topicConsumer = topicConsumer,
        eventual = eventual,
        pollTimeout = config.journal.pollTimeout,
        closeTimeout = config.journal.closeTimeout,
        readJournal = headCache)
    }
  }

  "Journal" should {

    for {
      seqNr <- List(SeqNr.Min, SeqNr(10))
      (eventualName, eventual) <- List(
        ("empty", () => EventualJournal.empty[Async]),
        ("non-empty", () => eventual))
    } {
      val name = s"seqNr: $seqNr, eventual: $eventualName"

      def keyOf() = Key(id = UUID.randomUUID().toString, topic = "journal")

      lazy val journal0 = journalOf(eventual())

      s"append, delete, read, lastSeqNr, $name" in {
        val key = keyOf()
        val journal = KeyJournal(key, journal0)
        val result = for {
          pointer <- journal.pointer()
          _ = pointer shouldEqual None
          events <- journal.read()
          _ = events shouldEqual Nil
          offset <- journal.delete(SeqNr.Max)
          _ = offset shouldEqual None
          event = Event(seqNr)
          offset <- journal.append(Nel(event))
          partition = offset.partition
          events <- journal.read()
          _ = events shouldEqual List(event)
          offset <- journal.delete(SeqNr.Max)
          _ = offset.map(_.partition) shouldEqual Some(partition)
          pointer <- journal.pointer()
          _ = pointer shouldEqual Some(seqNr)
          events <- journal.read()
          _ = events shouldEqual Nil
        } yield Succeeded

        result.future
      }

      val many = 10
      s"append & read $many, $name" in {
        val key = keyOf()
        val journal = KeyJournal(key, journal0)

        val events = for {
          n <- 0 until many
          seqNr <- seqNr.map(_ + n)
        } yield {
          Event(seqNr)
        }

        val result = for {
          _ <- journal.append(Nel.unsafe(events))
          _ <- Async.foldUnit {
            for {
              _ <- 0 to 10
            } yield for {
              events <- journal.read()
              _ = events shouldEqual events
              pointer <- journal.pointer()
              _ = pointer shouldEqual events.lastOption.map(_.seqNr)
            } yield {}
          }
        } yield Succeeded

        result.future
      }

      s"read $many in parallel, $name" in {

        val expected = for {
          n <- (0 to 10).toList
          seqNr <- seqNr.map(_ + n)
        } yield Event(seqNr)


        val result = for {
          recoveries <- Async.list {
            for {
              _ <- (0 until many).toList
            } yield {

              val key = keyOf()
              val journal = KeyJournal(key, journal0)

              for {
                events <- journal.read()
                _ = events shouldEqual Nil
                pointer <- journal.pointer()
                _ = pointer shouldEqual None
                _ <- Async.fold(expected, ()) { (_, event) =>
                  for {
                    _ <- journal.append(Nel(event))
                  } yield {}
                }
              } yield {
                () => {
                  for {
                    pointer <- journal.pointer()
                    _ = pointer shouldEqual expected.lastOption.map(_.seqNr)
                    events <- journal.read()
                    _ = events shouldEqual expected
                  } yield {}
                }
              }
            }
          }

          _ <- Async.foldUnit {
            for {
              recovery <- recoveries
            } yield {
              recovery()
            }
          }
        } yield Succeeded

        result.future
      }
    }
  }
}