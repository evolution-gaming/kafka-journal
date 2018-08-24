package com.evolutiongaming.kafka.journal.eventual

import java.time.Instant

import com.evolutiongaming.kafka.journal.FoldWhileHelper._
import com.evolutiongaming.kafka.journal.SeqNr.Helper._
import com.evolutiongaming.kafka.journal._
import com.evolutiongaming.nel.Nel
import com.evolutiongaming.skafka.Topic
import org.scalatest.Matchers._
import org.scalatest.{Matchers, WordSpec}

trait EventualJournalSpec extends WordSpec with Matchers {
  import EventualJournalSpec._

  def test(createJournals: () => Journals): Unit = {
    val journalsOf = (key: Key, timestamp: Instant) => {
      val journals = createJournals()
      val eventual = Eventual(journals.eventual, key)
      val replicated = Replicated(journals.replicated, key, timestamp)
      (eventual, replicated)
    }
    test(journalsOf)
  }

  private def test(journalsOf: (Key, Instant) => (Eventual, Replicated)): Unit = {

    val topic = "topic"
    val key = Key("id", topic)
    val timestamp = Instant.now()
    val createJournals = () => journalsOf(key, timestamp)

    for {
      size <- Nel(0, 1, 2, 5, 10)
    } {
      val name = s"[events:$size]"

      def eventsOf(from: SeqNr, size: Int) = {
        for {
          to <- SeqNr.opt(from.value + size).toList
          if from != to
          seqNr <- (from to to).seqNrs.toList
        } yield {
          eventOf(seqNr)
        }
      }

      val events = eventsOf(SeqNr.Min, size)
      val seqNrs = events.map(_.seqNr)
      val seqNrLast = seqNrs.lastOption
      val seqNrsAll = {
        val end = seqNrLast getOrElse SeqNr.Min
        val seqNrs = (SeqNr.Min to end).seqNrs
        SeqNr.Max :: seqNrs
      }

      def createAndAppend() = {
        val (eventual, replicated) = createJournals()

        for {events <- Nel.opt(events)} {
          replicated.append(events)
        }
        (eventual, replicated)
      }

      s"$name append" in {
        val (eventual, _) = createAndAppend()
        eventual.events() shouldEqual events
      }

      for {
        seqNrLast <- seqNrLast
        from <- seqNrLast.nextOpt
        size <- Nel(1, 5)
      } {
        s"$name append $size to existing journal" in {
          val (eventual, replicated) = createAndAppend()
          val head = eventsOf(from, size)
          for {head <- Nel.opt(head)} replicated.append(head)
          eventual.events() shouldEqual events ++ head
        }
      }

      def testDelete(deleteTo: SeqNr, unbound: Boolean) = {

        s"$name deleteTo $deleteTo, unbound: $unbound" in {
          val (eventual, replicated) = createAndAppend()
          replicated.delete(deleteTo, unbound)
          val expected = events.dropWhile(_.seqNr <= deleteTo)
          eventual.events() shouldEqual expected
          eventual.lastSeqNr() shouldEqual seqNrLast
        }

        s"$name deleteTo $deleteTo, unbound: $unbound and then append" in {
          val (eventual, replicated) = createAndAppend()
          replicated.delete(deleteTo, unbound)
          val expected = events.dropWhile(_.seqNr <= deleteTo)
          eventual.events() shouldEqual expected
          eventual.lastSeqNr() shouldEqual seqNrLast

          val event = {
            val seqNr = for {
              seqNr <- seqNrLast
              seqNr <- seqNr.nextOpt
            } yield seqNr
            eventOf(seqNr getOrElse SeqNr.Min)
          }
          replicated.append(Nel(event))
          eventual.events() shouldEqual expected ++ List(event)
          eventual.lastSeqNr() shouldEqual Some(event.seqNr)
        }
      }

      for {deleteTo <- seqNrs} {
        testDelete(deleteTo, unbound = false)
      }

      for {deleteTo <- seqNrsAll} {
        testDelete(deleteTo, unbound = true)
      }

      for {
        deleteTo <- seqNrLast
        deleteToUnbound <- List(deleteTo, SeqNr.Max)
      } {
        s"$name deleteTo $deleteTo and then deleteTo $deleteToUnbound" in {
          val (eventual, replicated) = createAndAppend()
          replicated.delete(deleteTo)
          eventual.events() shouldEqual Nil
          eventual.lastSeqNr() shouldEqual Some(deleteTo)

          replicated.delete(deleteToUnbound, unbound = true)
          eventual.events() shouldEqual Nil
          eventual.lastSeqNr() shouldEqual Some(deleteTo)
        }
      }

      {
        val (eventual, _) = createAndAppend()
        for {
          from <- seqNrsAll
        } {
          s"$name read from $from" in {
            val expected = events.dropWhile(_.seqNr < from)
            eventual.events(from) shouldEqual expected
          }

          s"$name lastSeqNr $from" in {
            val expected = seqNrLast.filter(_ >= from)
            eventual.lastSeqNr(from) shouldEqual expected
          }
        }
      }
    }

    for {
      size <- List(0, 1, 5)
    } {

      val name = s"[topics:$size]"

      val topics = for {idx <- 0 to size} yield s"topic-$idx"

      val (eventual, replicated) = createJournals()
      val pointers = TopicPointers(Map((1, 1l)))
      for {topic <- topics} {
        replicated.save(topic, pointers)
      }

      s"$name save pointers" in {
        for {topic <- topics} {
          replicated.pointers(topic) shouldEqual pointers
        }
      }

      s"$name topics" in {
        replicated.topics() shouldEqual topics
      }

      for {topic <- topics} {
        s"$name read pointers for $topic" in {
          replicated.pointers(topic) shouldEqual pointers
          eventual.pointers(topic) shouldEqual pointers
        }
      }

      s"$name read pointers for unknown" in {
        replicated.pointers("unknown") shouldEqual TopicPointers.Empty
        eventual.pointers("unknown") shouldEqual TopicPointers.Empty
      }
    }

    "save empty pointers empty" in {
      val (eventual, replicated) = createJournals()
      replicated.save(topic, TopicPointers.Empty)
      replicated.topics() shouldEqual Nil
      replicated.pointers(topic) shouldEqual TopicPointers.Empty
      eventual.pointers(topic) shouldEqual TopicPointers.Empty
    }


    "delete, append, (delete,append), append, delete" in {
      val (eventual, replicated) = createJournals()

      val event1 = eventOf(1.toSeqNr)
      val event2 = eventOf(2.toSeqNr)
      val event3 = eventOf(3.toSeqNr)

      eventual.events() shouldEqual Nil
      eventual.lastSeqNr() shouldEqual None

      replicated.delete(SeqNr.Max, unbound = true)
      eventual.events() shouldEqual Nil
      eventual.lastSeqNr() shouldEqual None

      replicated.append(Nel(event1))
      eventual.events() shouldEqual List(event1)
      eventual.lastSeqNr() shouldEqual Some(event1.seqNr)

      replicated.deleteAndAppend(event1.seqNr, Nel(event2))
      eventual.events() shouldEqual List(event2)
      eventual.lastSeqNr() shouldEqual Some(event2.seqNr)

      replicated.append(Nel(event3))
      eventual.events() shouldEqual List(event2, event3)
      eventual.lastSeqNr() shouldEqual Some(event3.seqNr)

      replicated.delete(event3.seqNr)
      eventual.events() shouldEqual Nil
      eventual.lastSeqNr() shouldEqual Some(event3.seqNr)

      replicated.delete(SeqNr.Max, unbound = true)
      eventual.events() shouldEqual Nil
      eventual.lastSeqNr() shouldEqual Some(event3.seqNr)
    }

    def eventOf(seqNr: SeqNr) = {
      val event = Event(seqNr)
      val partitionOffset = PartitionOffset(partition = 0, offset = seqNr.value * 2)
      ReplicatedEvent(event, timestamp, partitionOffset)
    }
  }
}

object EventualJournalSpec {

  trait Eventual {

    def events(from: SeqNr = SeqNr.Min): List[ReplicatedEvent]

    def lastSeqNr(from: SeqNr = SeqNr.Min): Option[SeqNr]

    def pointers(topic: Topic): TopicPointers
  }

  object Eventual {

    def apply(journal: EventualJournal, key: Key): Eventual = new Eventual {

      def events(from: SeqNr = SeqNr.Min) = {
        val async = journal.read(key, from, List.empty[ReplicatedEvent]) { (xs, x) => (x :: xs).continue }
        val switch = async.get()
        switch.continue shouldEqual true
        switch.s.reverse
      }

      def lastSeqNr(from: SeqNr = SeqNr.Min) = {
        journal.lastSeqNr(key, from).get()
      }

      def pointers(topic: Topic) = journal.pointers(topic).get()
    }
  }


  trait Replicated {

    def topics(): Iterable[Topic]

    def append(events: Nel[ReplicatedEvent]): Unit

    def delete(deleteTo: SeqNr, unbound: Boolean = false): Unit

    def deleteAndAppend(deleteTo: SeqNr, events: Nel[ReplicatedEvent]): Unit

    def pointers(topic: Topic): TopicPointers

    def save(topic: Topic, pointers: TopicPointers): Unit
  }

  object Replicated {

    def apply(journal: ReplicatedJournal, key: Key, timestamp: Instant): Replicated = new Replicated {

      def save(records: Replicate) = journal.save(key, records, timestamp).get()

      def topics() = journal.topics().get()

      def append(events: Nel[ReplicatedEvent]) = {
        save(Replicate.DeleteToKnown(None, events.toList))
      }

      def delete(deleteTo: SeqNr, unbound: Boolean = false) = {
        val replicate =
          if (unbound) Replicate.DeleteUnbound(deleteTo)
          else Replicate.DeleteToKnown(Some(deleteTo), Nil)
        save(replicate)
      }

      def deleteAndAppend(deleteTo: SeqNr, events: Nel[ReplicatedEvent]) = {
        save(Replicate.DeleteToKnown(Some(deleteTo), events.toList))
      }

      def pointers(topic: Topic) = journal.pointers(topic).get()

      def save(topic: Topic, pointers: TopicPointers) = journal.save(topic, pointers).get()
    }
  }


  final case class Journals(eventual: EventualJournal, replicated: ReplicatedJournal)
}