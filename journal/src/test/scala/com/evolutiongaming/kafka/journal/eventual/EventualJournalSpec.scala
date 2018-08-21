package com.evolutiongaming.kafka.journal.eventual

import java.time.Instant

import com.evolutiongaming.kafka.journal.FoldWhileHelper._
import com.evolutiongaming.kafka.journal.SeqNr.Helper._
import com.evolutiongaming.kafka.journal._
import com.evolutiongaming.nel.Nel
import org.scalatest.{Matchers, WordSpec}

trait EventualJournalSpec extends WordSpec with Matchers {

  // TODO test offsets
  def test(createJournals: () => Journals): Unit = {

    val topic = "topic"
    val key = Key("id", topic)

    for {
      size <- Nel(0, 1, 2, 5, 10)
    } {
      val name = s"[events:$size]"
      val timestamp = Instant.now()

      val events = for {
        seqNr <- (1L to size.toLong).toList.map(_.toSeqNr) // TODO convert to SeqRange // Use NEL
      } yield {
        val event = Event(seqNr)
        val shift = 10
        val partitionOffset = PartitionOffset(partition = 0, offset = seqNr.value + shift)
        ReplicatedEvent(event, timestamp, partitionOffset)
      }
      val seqNrs = events.map(_.event.seqNr)
      val seqNrLast = seqNrs.lastOption
      val seqNrsAll = {
        val end = seqNrLast getOrElse SeqNr.Min
        val seqNrs = (SeqNr.Min to end).seqNrs
        SeqNr.Max :: seqNrs
      }

      def createAndAppend() = {
        val journals = createJournals()
        val eventual = journals.eventual
        val replicate = journals.replicate

        for {
          events <- Nel.opt(events)
        } {
          // TODO use Nel
          val replicated = Replicate.DeleteToKnown(None, events.toList)
          replicate.save(key, replicated, timestamp)
        }
        (eventual, replicate)
      }

      s"$name append" in {
        val (eventual, replicate) = createAndAppend()
        eventual.events(key, SeqNr.Min) shouldEqual events
      }

      for {
        deleteTo <- seqNrs
      } {
        s"$name deleteTo $deleteTo" in {
          val (eventual, replicate) = createAndAppend()
          replicate.save(key, Replicate.DeleteToKnown(Some(deleteTo), Nil), timestamp)
          val expected = events.dropWhile(_.event.seqNr <= deleteTo)
          eventual.events(key, SeqNr.Min) shouldEqual expected
          eventual.lastSeqNr(key, SeqNr.Min).get() shouldEqual seqNrLast
        }
      }

      for {
        deleteTo <- seqNrsAll
      } {
        s"$name deleteTo $deleteTo unbound" in {
          val (eventual, replicate) = createAndAppend()
          replicate.save(key, Replicate.DeleteUnbound(deleteTo), timestamp)
          val expected = events.dropWhile(_.event.seqNr <= deleteTo)
          eventual.events(key, SeqNr.Min) shouldEqual expected
          eventual.lastSeqNr(key, SeqNr.Min).get() shouldEqual seqNrLast
        }
      }

      {
        val (eventual, replicate) = createAndAppend()
        for {
          from <- seqNrsAll
        } {
          s"$name read from $from" in {
            val expected = events.dropWhile(_.event.seqNr < from)
            eventual.events(key, from) shouldEqual expected
          }

          s"$name lastSeqNr $from" in {
            val expected = seqNrLast.filter(_ >= from)
            eventual.lastSeqNr(key, from).get() shouldEqual expected
          }
        }
      }
    }

    for {
      size <- List(0, 1, 5)
    } {

      val name = s"[topics:$size]"

      val topics = for {idx <- 0 to size} yield s"topic-$idx"

      val Journals(eventual, replicate) = createJournals()
      val pointers = TopicPointers(Map((1, 1l)))
      for {topic <- topics} {
        replicate.savePointers(topic, pointers)
      }

      s"$name save pointers" in {
        for {topic <- topics} {
          replicate.pointers(topic).get() shouldEqual pointers
        }
      }

      s"$name topics" in {
        replicate.topics().get() shouldEqual topics
      }

      for {topic <- topics} {
        s"$name read pointers for $topic" in {
          replicate.pointers(topic).get() shouldEqual pointers
          eventual.pointers(topic).get() shouldEqual pointers
        }
      }

      s"$name read pointers for unknown" in {
        replicate.pointers("unknown").get() shouldEqual TopicPointers.Empty
        eventual.pointers("unknown").get() shouldEqual TopicPointers.Empty
      }
    }

    "save empty pointers empty" in {
      val Journals(eventual, replicate) = createJournals()
      replicate.savePointers(topic, TopicPointers.Empty)
      replicate.topics().get() shouldEqual Nil
      replicate.pointers(topic).get() shouldEqual TopicPointers.Empty
      eventual.pointers(topic).get() shouldEqual TopicPointers.Empty
    }
  }

  case class Journals(eventual: EventualJournal, replicate: ReplicatedJournal)


  implicit class EventualJournalOps(val self: EventualJournal) {

    def events(key: Key, from: SeqNr): List[ReplicatedEvent] = {
      val async = self.read(key, from, List.empty[ReplicatedEvent]) { (xs, x) => (x :: xs).continue }
      val switch = async.get()
      switch.continue shouldEqual true
      switch.s.reverse
    }
  }
}