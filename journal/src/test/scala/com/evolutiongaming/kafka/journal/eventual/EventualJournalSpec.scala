package com.evolutiongaming.kafka.journal.eventual

import java.time.Instant

import cats.data.{NonEmptyList => Nel, NonEmptyMap => Nem}
import cats.effect.Clock
import cats.implicits._
import cats.{Applicative, FlatMap, Monad}
import com.evolutiongaming.catshelper.ClockHelper._
import com.evolutiongaming.catshelper.{Log, MonadThrowable}
import com.evolutiongaming.kafka.journal._
import com.evolutiongaming.kafka.journal.util.OptionHelper._
import com.evolutiongaming.kafka.journal.util.CatsHelper._
import com.evolutiongaming.skafka.{Offset, Partition, Topic}
import com.evolutiongaming.smetrics.MeasureDuration
import org.scalatest.{Assertion, Matchers, WordSpec}
import play.api.libs.json.Json


trait EventualJournalSpec extends WordSpec with Matchers {
  import EventualJournalSpec._

  def test[F[_] : MonadThrowable](withJournals: (Journals[F] => F[Assertion]) => F[Assertion]): Unit = {

    val withJournals1 = (key: Key, timestamp: Instant) => {

      (f: (Eventual[F], Replicated[F]) => F[Assertion]) => {
        withJournals { journals =>
          implicit val log = Log.empty[F]
          implicit val clock = Clock.const[F](nanos = 0, millis = 0)
          implicit val measureDuration = MeasureDuration.fromClock(clock)
          val eventual = {
            val journal = journals.eventual
              .withLog(log)
              .withMetrics(EventualJournal.Metrics.empty[F])
            Eventual[F](journal, key)
          }
          val replicated = {
            val journal = journals.replicated
              .withLog(log)
              .withMetrics(ReplicatedJournal.Metrics.empty[F])
            Replicated[F](journal, key, timestamp)
          }
          f(eventual, replicated)
        }
      }
    }

    test1(withJournals1)
  }

  private def test1[F[_] : MonadThrowable](
    withJournals: (Key, Instant) => ((Eventual[F], Replicated[F]) => F[Assertion]) => F[Assertion]
  ): Unit = {

    implicit val monoidUnit = Applicative.monoid[F, Unit]

    val key = EventualJournalSpec.key

    def withJournals1(f: (Eventual[F], Replicated[F]) => F[Assertion]): F[Assertion] = {
      withJournals(key, timestamp) { case (eventual, replicated) =>
        f(eventual, replicated)
      }
    }

    def eventOf(pointer: JournalPointer): EventRecord = {
      val event = Event(pointer.seqNr)
      val recordMetadata = RecordMetadata(data = Some(Json.obj(("key", "value"))))
      val headers = Headers(("key", "value"))
      EventRecord(
        event = event,
        timestamp = timestamp,
        partitionOffset = pointer.partitionOffset,
        metadata = recordMetadata,
        headers = headers)
    }

    for {
      seqNr <- List(SeqNr.min, SeqNr.unsafe(5), SeqNr.unsafe(10))
      size  <- List(0, 1, 2, 5, 10)
      batch <- List(true, false)
    } {
      val name = s"seqNr: $seqNr, events: $size, batch: $batch"

      def eventsOf(from: JournalPointer, size: Int) = {
        for {
          n       <- (0 until size).toList
          seqNr   <- SeqNr.opt(from.seqNr.value + n).toList
          offset   = from.offset + n
          pointer  = journalPointerOf(offset, seqNr)
        } yield {
          eventOf(pointer)
        }
      }

      val events = eventsOf(journalPointerOf(Offset.Min, seqNr), size)
      val pointers = events.map(_.pointer)
      val pointerLast = pointers.lastOption
      val partitionOffsetNext = pointerLast.fold(partitionOffsetOf(Offset.Min))(_.partitionOffset.next)
      val seqNrsAll = {
        val end = pointerLast.fold(seqNr)(_.seqNr)
        val seqNrs = (seqNr to end).toNel
        (SeqNr.min :: SeqNr.max :: seqNrs).distinct
      }


      def withJournals2(f: (Eventual[F], Replicated[F]) => F[Assertion]): F[Assertion] = {
        withJournals1 { case (eventual, replicated) =>
          for {
            _ <- {
              if (batch) {
                events.toNel.foldMap { events => replicated.append(events) }
              } else {
                events.foldMap { event => replicated.append(Nel.of(event)) }
              }
            }
            result <- f(eventual, replicated)
          } yield result
        }
      }

      s"append, $name" in {
        withJournals2 { case (eventual, _) =>
          for {
            a <- eventual.events()
          } yield {
            a shouldEqual events
          }
        }
      }

      for {
        seqNrLast <- pointerLast
        from      <- seqNrLast.next
        size      <- List(1, 5)
      } {
        s"append $size to existing journal, $name" in {
          withJournals2 { case (eventual, replicated) =>
            val head = eventsOf(from, size)
            for {
              _ <- head.toNel.foldMap { head => replicated.append(head) }
              a <- eventual.events()
            } yield {
              a shouldEqual events ++ head
            }
          }
        }
      }

      for {
        pointer <- pointers
        deleteTo = pointer.seqNr
      } {
        s"delete to $deleteTo, $name" in {
          withJournals2 { case (eventual, replicated) =>
            for {
              _        <- replicated.delete(deleteTo, partitionOffsetNext)
              expected  = events.dropWhile(_.seqNr <= deleteTo)
              a        <- eventual.events()
              _         = a shouldEqual expected
              a         <- eventual.pointer
            } yield {
              a shouldEqual pointerLast.map(_.withPartitionOffset(partitionOffsetNext))
            }
          }
        }

        s"delete to $deleteTo and then append, $name" in {
          withJournals2 { case (eventual, replicated) =>
            for {
              _        <- replicated.delete(deleteTo, partitionOffsetNext)
              expected  = events.dropWhile(_.seqNr <= deleteTo)
              a        <- eventual.events()
              _         = a shouldEqual expected
              pointer  <- eventual.pointer
              _         = pointer shouldEqual pointerLast.map(_.withPartitionOffset(partitionOffsetNext))
              event     = {
                val seqNr = for {
                  seqNr <- pointerLast
                  seqNr <- seqNr.next
                } yield seqNr
                eventOf(seqNr getOrElse JournalPointer.min)
              }
              pointer  <- pointer.next.getOrError[F]("pointer.next")
              event    <- event.copy(partitionOffset = pointer.partitionOffset).pure[F]
              _        <- replicated.append(Nel.of(event))
              a        <- eventual.events()
              _         = a shouldEqual expected ++ List(event)
              a        <- eventual.pointer
            } yield {
              a shouldEqual Some(event.pointer)
            }
          }
        }
      }

      for {
        pointer  <- pointerLast
        deleteTo  = pointer.seqNr
      } {
        s"deleteTo $deleteTo and then deleteTo $deleteTo, $name" in {
          withJournals2 { case (eventual, replicated) =>
            for {
              _                <- replicated.delete(deleteTo, partitionOffsetNext)
              a                <- eventual.events()
              _                 = a shouldEqual Nil
              a                <- eventual.pointer
              _                 = a shouldEqual Some(JournalPointer(partitionOffsetNext, deleteTo))
              partitionOffset2  = partitionOffsetNext.next
              _                <- replicated.delete(deleteTo, partitionOffset2)
              a                <- eventual.events()
              _                 = a shouldEqual Nil
              a                <- eventual.pointer
            } yield {
              a shouldEqual Some(JournalPointer(partitionOffset2, deleteTo))
            }
          }
        }
      }


      for {
        from <- seqNrsAll.toList
      } {
        s"read from: $from, $name" in {
          val expected = events.dropWhile(_.seqNr < from)

          withJournals2 { case (eventual, _) =>
            for {
              a <- eventual.events(from)
            } yield {
              a shouldEqual expected
            }
          }
        }
      }

      s"pointer, $name" in {
        withJournals2 { case (eventual, _) =>
          for {
            a <- eventual.pointer
          } yield {
            a shouldEqual pointerLast
          }
        }
      }
    }

    for {
      size <- List(0, 1, 5)
    } {

      val partitionOffset = PartitionOffset(partition = 1, offset = 1)
      val pointers = Nem.of((partitionOffset.partition, partitionOffset.offset))
      val topicPointers = TopicPointers(Map((partitionOffset.partition, partitionOffset.offset)))

      val name = s" topics: $size"

      val topics = for {idx <- 0 to size} yield s"topic-$idx"

      def withJournals3(f: (Eventual[F], Replicated[F]) => F[Assertion]): F[Assertion] = {
        withJournals1 { case (eventual, replicated) =>
          for {
            _      <- topics.toList.foldMapM { topic => replicated.save(topic, pointers) }
            result <- f(eventual, replicated)
          } yield result
        }
      }

      for {
        topic <- topics
      } {
        s"save pointers, $name, topic: $topic" in {
          withJournals3 { case (_, replicated) =>
            for {
              a <- replicated.pointers(topic)
            } yield {
              a shouldEqual topicPointers
            }
          }
        }
      }

      s"topics, $name" in {
        withJournals3 { case (_, replicated) =>
          for {
            a <- replicated.topics
          } yield {
            a shouldEqual topics
          }
        }
      }

      for {
        topic <- topics
      } {
        s"read pointers for $topic, $name " in {
          withJournals3 { case (eventual, replicated) =>
            for {
              a <- replicated.pointers(topic)
              _ = a shouldEqual topicPointers
              a <- eventual.pointers(topic)
            } yield {
              a shouldEqual topicPointers
            }
          }
        }
      }

      s"read pointers for unknown, $name " in {
        withJournals3 { case (eventual, replicated) =>
          for {
            a <- replicated.pointers("unknown")
            _  = a shouldEqual TopicPointers.empty
            a <- eventual.pointers("unknown")
          } yield {
            a shouldEqual TopicPointers.empty
          }
        }
      }
    }

    for {
      deleteTo <- List(1, 2, 5)
      deleteTo <- SeqNr.opt(deleteTo.toLong)
      deleteToPointer = journalPointerOf(offset = 1, seqNr = deleteTo)
    } {
      s"deleteTo $deleteTo on empty journal" in {
        withJournals1 { case (eventual, replicated) =>
          for {
            _ <- replicated.delete(deleteTo, deleteToPointer.partitionOffset)
            a <- eventual.events()
            _  = a shouldEqual Nil
            a <- eventual.pointer
          } yield {
            a shouldEqual Some(deleteToPointer)
          }
        }
      }

      s"deleteTo $deleteTo on empty journal and then append" in {
        withJournals1 { case (eventual, replicated) =>
          for {
            _     <- replicated.delete(deleteTo, deleteToPointer.partitionOffset)
            a     <- eventual.events()
            _      = a shouldEqual Nil
            event  = {
              val pointerNext = deleteToPointer.next getOrElse JournalPointer.min
              eventOf(pointerNext)
            }
            _     <- replicated.append(Nel.of(event))
            a     <- eventual.events()
            _      = a shouldEqual List(event)
            a     <- eventual.pointer
          } yield {
            a shouldEqual Some(event.pointer)
          }
        }
      }
    }


    "delete, append, delete, append, append, delete" in {
      withJournals1 { case (eventual, replicated) =>
        for {
          a      <- eventual.events()
          _       = a shouldEqual Nil
          a      <- eventual.pointer
          _       = a shouldEqual None
          event1  = eventOf(journalPointerOf(offset = 2, seqNr = SeqNr.unsafe(1)))
          _      <- replicated.append(Nel.of(event1))
          a      <- eventual.events()
          _       = a shouldEqual List(event1)
          a      <- eventual.pointer
          _       = a shouldEqual Some(event1.pointer)
          event2  = eventOf(journalPointerOf(offset = 3, seqNr = SeqNr.unsafe(2)))
          _      <- replicated.append(partitionOffsetOf(4), Nel.of(event2))
          a      <- eventual.events()
          _       = a shouldEqual List(event1, event2)
          a      <- eventual.pointer
          _       = a shouldEqual Some(journalPointerOf(offset = 4, seqNr = event2.seqNr))
          _      <- replicated.delete(event1.seqNr, partitionOffsetOf(5))
          a      <- eventual.events()
          _       = a shouldEqual List(event2)
          a      <- eventual.pointer
          _       = a shouldEqual Some(journalPointerOf(offset = 5, seqNr = event2.seqNr))
          event3  = eventOf(journalPointerOf(offset = 6, seqNr = SeqNr.unsafe(3)))
          _      <- replicated.append(Nel.of(event3))
          a      <- eventual.events()
          _       = a shouldEqual List(event2, event3)
          a      <- eventual.pointer
          _       = a shouldEqual Some(event3.pointer)
          _      <- replicated.delete(event3.seqNr, partitionOffsetOf(7))
          a      <- eventual.events()
          _       = a shouldEqual Nil
          a      <- eventual.pointer
          _       = a shouldEqual Some(journalPointerOf(offset = 7, seqNr = event3.seqNr))
          _      <- replicated.delete(SeqNr.max, partitionOffsetOf(8))
          a      <- eventual.events()
          _       = a shouldEqual Nil
          a      <- eventual.pointer
        } yield {
          a shouldEqual Some(journalPointerOf(offset = 8, seqNr = SeqNr.max))
        }
      }
    }

    "append not from beginning" in {
      val events = Nel.of(
        eventOf(journalPointerOf(offset = 10, seqNr = SeqNr.unsafe(5))),
        eventOf(journalPointerOf(offset = 10, seqNr = SeqNr.unsafe(6))),
        eventOf(journalPointerOf(offset = 10, seqNr = SeqNr.unsafe(7))))

      withJournals1 { case (eventual, replicated) =>
        for {
          _ <- replicated.append(events)
          a <- eventual.pointer
          _  = a shouldEqual Some(events.last.pointer)
          a <- eventual.events(SeqNr.min)
        } yield {
          a shouldEqual events.toList
        }
      }
    }
  }
}

object EventualJournalSpec {

  val topic: Topic = "topic"

  val key: Key = Key("id", topic)

  val timestamp: Instant = Instant.now()

  def partitionOffsetOf(offset: Offset): PartitionOffset = PartitionOffset(offset = offset)

  def journalPointerOf(offset: Offset, seqNr: SeqNr): JournalPointer = {
    JournalPointer(partitionOffsetOf(offset), seqNr)
  }

  trait Eventual[F[_]] {

    def events(from: SeqNr = SeqNr.min): F[List[EventRecord]]

    def pointer: F[Option[JournalPointer]]

    def pointers(topic: Topic): F[TopicPointers]
  }

  object Eventual {

    def apply[F[_] : Monad](journal: EventualJournal[F], key: Key): Eventual[F] = new Eventual[F] {

      def events(from: SeqNr = SeqNr.min) = {
        journal.read(key, from).toList
      }

      def pointer = journal.pointer(key)

      def pointers(topic: Topic) = journal.pointers(topic)
    }
  }


  trait Replicated[F[_]] {

    def topics: F[Iterable[Topic]]

    final def append(events: Nel[EventRecord]): F[Unit] = {
      val partitionOffset = events.last.partitionOffset // TODO add test for custom offset
      append(partitionOffset, events)
    }

    def append(partitionOffset: PartitionOffset, events: Nel[EventRecord]): F[Unit]

    def delete(deleteTo: SeqNr, partitionOffset: PartitionOffset): F[Unit]

    def pointers(topic: Topic): F[TopicPointers]

    def save(topic: Topic, pointers: Nem[Partition, Offset]): F[Unit]
  }

  object Replicated {

    def apply[F[_] : FlatMap](
      journal: ReplicatedJournal[F],
      key: Key,
      timestamp: Instant
    ): Replicated[F] = {
      new Replicated[F] {

        def topics = journal.topics

        def append(partitionOffset: PartitionOffset, events: Nel[EventRecord]) = {
          // TODO expireAfter: define and test
          journal.append(key, partitionOffset, timestamp, none, events)
        }

        def delete(deleteTo: SeqNr, partitionOffset: PartitionOffset) = {
          journal.delete(key, partitionOffset, timestamp, deleteTo, None)
        }

        def pointers(topic: Topic) = journal.pointers(topic)

        def save(topic: Topic, pointers: Nem[Partition, Offset]) = {
          journal.save(topic, pointers, timestamp)
        }
      }
    }
  }


  final case class Journals[F[_]](eventual: EventualJournal[F], replicated: ReplicatedJournal[F])


  implicit class JournalPointerOps(val self: JournalPointer) extends AnyVal {

    def next: Option[JournalPointer] = {
      for {
        seqNr <- self.seqNr.next[Option]
      } yield {
        val partitionOffset = self.partitionOffset
        self.copy(
          seqNr = seqNr,
          partitionOffset = partitionOffset.copy(offset = partitionOffset.offset + 1))
      }
    }

    def withPartitionOffset(partitionOffset: PartitionOffset): JournalPointer = {
      self.copy(partitionOffset = partitionOffset)
    }
  }


  implicit class JournalPointerOptionOps(val self: Option[JournalPointer]) extends AnyVal {

    def next: Option[JournalPointer] = self.flatMap(_.next)
  }


  implicit class JournalPointerObjOps(val self: JournalPointer.type) extends AnyVal {

    def min: JournalPointer = journalPointerOf(Offset.Min, SeqNr.min)
  }


  implicit class PartitionOffsetOps(val self: PartitionOffset) extends AnyVal {
    def next: PartitionOffset = self.copy(offset = self.offset + 1)
  }
}