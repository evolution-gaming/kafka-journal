package com.evolutiongaming.kafka.journal.eventual

import java.time.Instant

import com.evolutiongaming.concurrent.async.Async
import com.evolutiongaming.kafka.journal.AsyncHelper._
import com.evolutiongaming.kafka.journal.FoldWhile._
import com.evolutiongaming.kafka.journal._
import com.evolutiongaming.nel.Nel
import com.evolutiongaming.safeakka.actor.ActorLog
import com.evolutiongaming.skafka.{Offset, Topic}
import org.scalatest.Matchers._
import org.scalatest.{Matchers, WordSpec}

trait EventualJournalSpec extends WordSpec with Matchers {
  import EventualJournalSpec._

  def test(createJournals: () => Journals): Unit = {
    val journalsOf = (key: Key, timestamp: Instant) => {
      val journals = createJournals()
      val eventual = {
        val journal = journals.eventual
        val withLogging = EventualJournal(journal, ActorLog.empty)
        val metrics = EventualJournal.Metrics.empty(Async.unit)
        val withMetrics = EventualJournal(withLogging, metrics)
        Eventual(withMetrics, key)
      }
      val replicated = {
        val journal = journals.replicated
        val withLogging = ReplicatedJournal(journal, Log.empty(Async.unit))
        val metrics = ReplicatedJournal.Metrics.empty(Async.unit)
        val withMetrics = ReplicatedJournal(withLogging, metrics)
        Replicated(withMetrics, key, timestamp)
      }
      (eventual, replicated)
    }
    test(journalsOf)
  }

  private def test(journalsOf: (Key, Instant) => (Eventual, Replicated)): Unit = {
    val topic = "topic"
    val key = Key("id", topic)
    val timestamp = Instant.now()
    val createJournals = () => journalsOf(key, timestamp)

    def eventOf(pointer: Pointer): ReplicatedEvent = {
      val event = Event(pointer.seqNr)
      ReplicatedEvent(event, timestamp, pointer.partitionOffset)
    }

    for {
      seqNr <- List(SeqNr.Min, SeqNr(5), SeqNr(10))
      size <- Nel(0, 1, 2, 5, 10)
      batch <- List(true, false)
    } {
      val name = s"seqNr: $seqNr, events: $size, batch: $batch"

      def eventsOf(from: Pointer, size: Int) = {
        for {
          n <- (0 until size).toList
          seqNr <- SeqNr.opt(from.seqNr.value + n).toList
          offset = from.offset + n
        } yield {
          val pointer = pointerOf(offset, seqNr)
          eventOf(pointer)
        }
      }

      val events = eventsOf(pointerOf(Offset.Min, seqNr), size)
      val pointers = events.map(_.pointer)
      val pointerLast = pointers.lastOption
      val partitionOffsetNext = pointerLast.fold(partitionOffsetOf(Offset.Min))(_.partitionOffset.next)
      val seqNrsAll = {
        val end = pointerLast.fold(seqNr)(_.seqNr)
        val seqNrs = (seqNr to end).seqNrs
        (SeqNr.Min :: SeqNr.Max :: seqNrs).distinct
      }

      def createAndAppend() = {
        val (eventual, replicated) = createJournals()

        if (batch) {
          for {
            events <- Nel.opt(events)
          } replicated.append(events)
        } else {
          for {
            event <- events
          } replicated.append(Nel(event))
        }

        (eventual, replicated)
      }

      s"append, $name" in {
        val (eventual, _) = createAndAppend()
        eventual.events() shouldEqual events
      }

      for {
        seqNrLast <- pointerLast
        from <- seqNrLast.next
        size <- Nel(1, 5)
      } {
        s"append $size to existing journal, $name" in {
          val (eventual, replicated) = createAndAppend()
          val head = eventsOf(from, size)
          for {head <- Nel.opt(head)} replicated.append(head)
          eventual.events() shouldEqual events ++ head
        }
      }

      for {
        pointer <- pointers
        deleteTo = pointer.seqNr
      } {
        s"deleteTo $deleteTo, $name" in {
          val (eventual, replicated) = createAndAppend()
          replicated.delete(deleteTo, partitionOffsetNext)
          val expected = events.dropWhile(_.seqNr <= deleteTo)
          eventual.events() shouldEqual expected
          eventual.pointer() shouldEqual pointerLast.map(_.withPartitionOffset(partitionOffsetNext))
        }

        s"deleteTo $deleteTo and then append, $name" in {
          val (eventual, replicated) = createAndAppend()
          replicated.delete(deleteTo, partitionOffsetNext)
          val expected = events.dropWhile(_.seqNr <= deleteTo)
          eventual.events() shouldEqual expected
          eventual.pointer() shouldEqual pointerLast.map(_.withPartitionOffset(partitionOffsetNext))

          val event = {
            val seqNr = for {
              seqNr <- pointerLast
              seqNr <- seqNr.next
            } yield seqNr
            eventOf(seqNr getOrElse Pointer.min)
          }
          replicated.append(Nel(event))
          eventual.events() shouldEqual expected ++ List(event)
          eventual.pointer() shouldEqual Some(event.pointer)
        }
      }

      for {
        pointer <- pointerLast
        deleteTo = pointer.seqNr
      } {
        s"deleteTo $deleteTo and then deleteTo $deleteTo, $name" in {
          val (eventual, replicated) = createAndAppend()
          replicated.delete(deleteTo, partitionOffsetNext)
          eventual.events() shouldEqual Nil
          eventual.pointer() shouldEqual Some(Pointer(partitionOffsetNext, deleteTo))

          val partitionOffset2 = partitionOffsetNext.next
          replicated.delete(deleteTo, partitionOffset2)
          eventual.events() shouldEqual Nil
          eventual.pointer() shouldEqual Some(Pointer(partitionOffset2, deleteTo))
        }
      }

      {
        val (eventual, _) = createAndAppend()
        for {
          from <- seqNrsAll
        } {
          s"read from: $from, $name" in {
            val expected = events.dropWhile(_.seqNr < from)
            eventual.events(from) shouldEqual expected
          }

          s"pointer: $from, $name" in {
            val expected = pointerLast.filter(_.seqNr >= from)
            eventual.pointer(from) shouldEqual expected
          }
        }
      }
    }

    for {
      size <- List(0, 1, 5)
    } {

      val name = s" topics: $size"

      val topics = for {idx <- 0 to size} yield s"topic-$idx"

      val (eventual, replicated) = createJournals()
      val partitionOffset = PartitionOffset(partition = 1, offset = 1)
      val pointers = TopicPointers(Map((partitionOffset.partition, partitionOffset.offset)))
      for {topic <- topics} {
        replicated.save(topic, pointers)
      }

      s"save pointers, $name" in {
        for {topic <- topics} {
          replicated.pointers(topic) shouldEqual pointers
        }
      }

      s"topics, $name " in {
        replicated.topics() shouldEqual topics
      }

      for {topic <- topics} {
        s"read pointers for $topic, $name " in {
          replicated.pointers(topic) shouldEqual pointers
          eventual.pointers(topic) shouldEqual pointers
        }
      }

      s"read pointers for unknown, $name " in {
        replicated.pointers("unknown") shouldEqual TopicPointers.Empty
        eventual.pointers("unknown") shouldEqual TopicPointers.Empty
      }
    }

    for {
      deleteTo <- List(1, 2, 5)
      deleteTo <- SeqNr.opt(deleteTo.toLong)
      deleteToPointer = pointerOf(offset = 1, seqNr = deleteTo)
    } {
      s"deleteTo $deleteTo on empty journal" in {
        val (eventual, replicated) = createJournals()
        replicated.delete(deleteTo, deleteToPointer.partitionOffset)
        eventual.events() shouldEqual Nil
        eventual.pointer() shouldEqual Some(deleteToPointer)
      }

      s"deleteTo $deleteTo on empty journal and then append" in {
        val (eventual, replicated) = createJournals()
        replicated.delete(deleteTo, deleteToPointer.partitionOffset)
        eventual.events() shouldEqual Nil

        val event = {
          val pointerNext = deleteToPointer.next getOrElse Pointer.min
          eventOf(pointerNext)
        }
        replicated.append(Nel(event))

        eventual.events() shouldEqual List(event)
        eventual.pointer() shouldEqual Some(event.pointer)
      }
    }

    "save empty pointers empty" in {
      val (eventual, replicated) = createJournals()
      replicated.save(topic, TopicPointers.Empty)
      replicated.topics() shouldEqual Nil
      replicated.pointers(topic) shouldEqual TopicPointers.Empty
      eventual.pointers(topic) shouldEqual TopicPointers.Empty
    }


    "delete, append, delete, append, append, delete" in {
      val (eventual, replicated) = createJournals()

      eventual.events() shouldEqual Nil
      eventual.pointer() shouldEqual None

      val event1 = eventOf(pointerOf(offset = 2, seqNr = SeqNr(1)))
      replicated.append(Nel(event1))
      eventual.events() shouldEqual List(event1)
      eventual.pointer() shouldEqual Some(event1.pointer)

      val event2 = eventOf(pointerOf(offset = 3, seqNr = SeqNr(2)))

      replicated.append(partitionOffsetOf(4), Nel(event2))
      eventual.events() shouldEqual List(event1, event2)
      eventual.pointer() shouldEqual Some(pointerOf(offset = 4, seqNr = event2.seqNr))

      replicated.delete(event1.seqNr, partitionOffsetOf(5))
      eventual.events() shouldEqual List(event2)
      eventual.pointer() shouldEqual Some(pointerOf(offset = 5, seqNr = event2.seqNr))

      val event3 = eventOf(pointerOf(offset = 6, seqNr = SeqNr(3)))
      replicated.append(Nel(event3))
      eventual.events() shouldEqual List(event2, event3)
      eventual.pointer() shouldEqual Some(event3.pointer)

      replicated.delete(event3.seqNr, partitionOffsetOf(7))
      eventual.events() shouldEqual Nil
      eventual.pointer() shouldEqual Some(pointerOf(offset = 7, seqNr = event3.seqNr))

      replicated.delete(SeqNr.Max, partitionOffsetOf(8))
      eventual.events() shouldEqual Nil
      eventual.pointer() shouldEqual Some(pointerOf(offset = 8, seqNr = SeqNr.Max))
    }

    "append not from beginning" in {
      val (eventual, replicated) = createJournals()
      val events = Nel(
        eventOf(pointerOf(offset = 10, seqNr = SeqNr(5))),
        eventOf(pointerOf(offset = 10, seqNr = SeqNr(6))),
        eventOf(pointerOf(offset = 10, seqNr = SeqNr(7))))
      replicated.append(events)
      eventual.pointer(SeqNr.Min) shouldEqual Some(events.last.pointer)
      eventual.events(SeqNr.Min) shouldEqual events.toList
    }
  }
}

object EventualJournalSpec {

  def partitionOffsetOf(offset: Offset): PartitionOffset = PartitionOffset(offset = offset)

  def pointerOf(offset: Offset, seqNr: SeqNr): Pointer = {
    Pointer(partitionOffsetOf(offset), seqNr)
  }

  trait Eventual {

    def events(from: SeqNr = SeqNr.Min): List[ReplicatedEvent]

    def pointer(from: SeqNr = SeqNr.Min): Option[Pointer]

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

      def pointer(from: SeqNr = SeqNr.Min) = {
        journal.pointer(key, from).get()
      }

      def pointers(topic: Topic) = journal.pointers(topic).get()
    }
  }


  trait Replicated {

    def topics(): Iterable[Topic]

    final def append(events: Nel[ReplicatedEvent]): Unit = {
      val partitionOffset = events.last.partitionOffset // TODO add test for custom offset
      append(partitionOffset, events)
    }

    def append(partitionOffset: PartitionOffset, events: Nel[ReplicatedEvent]): Unit

    def delete(deleteTo: SeqNr, partitionOffset: PartitionOffset): Unit

    def pointers(topic: Topic): TopicPointers

    def save(topic: Topic, pointers: TopicPointers): Unit
  }

  object Replicated {

    def apply(journal: ReplicatedJournal[Async], key: Key, timestamp: Instant): Replicated = new Replicated {

      def topics() = journal.topics().get()

      def append(partitionOffset: PartitionOffset, events: Nel[ReplicatedEvent]) = {
        journal.append(key, partitionOffset, timestamp, events)
      }

      def delete(deleteTo: SeqNr, partitionOffset: PartitionOffset) = {
        journal.delete(key, partitionOffset, timestamp, deleteTo, None)
      }

      def pointers(topic: Topic) = journal.pointers(topic).get()

      def save(topic: Topic, pointers: TopicPointers) = {
        journal.save(topic, pointers, timestamp).get()
      }
    }
  }


  final case class Journals(eventual: EventualJournal, replicated: ReplicatedJournal[Async])


  implicit class PointerOps(val self: Pointer) extends AnyVal {

    def next: Option[Pointer] = {
      for {
        seqNr <- self.seqNr.next
      } yield {
        val partitionOffset = self.partitionOffset
        self.copy(
          seqNr = seqNr,
          partitionOffset = partitionOffset.copy(offset = partitionOffset.offset + 1))
      }
    }

    def withPartitionOffset(partitionOffset: PartitionOffset): Pointer = {
      self.copy(partitionOffset = partitionOffset)
    }
  }


  implicit class PointerObjOps(val self: Pointer.type) extends AnyVal {
    def min: Pointer = pointerOf(Offset.Min, SeqNr.Min)
  }


  implicit class PartitionOffsetOps(val self: PartitionOffset) extends AnyVal {
    def next: PartitionOffset = self.copy(offset = self.offset + 1)
  }
}