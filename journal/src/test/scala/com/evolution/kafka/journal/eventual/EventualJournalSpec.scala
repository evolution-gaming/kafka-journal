package com.evolution.kafka.journal.eventual

import cats.data.NonEmptyList as Nel
import cats.effect.Clock
import cats.syntax.all.*
import cats.{Applicative, FlatMap, Monad, Monoid}
import com.evolutiongaming.catshelper.ClockHelper.*
import com.evolutiongaming.catshelper.DataHelper.*
import com.evolutiongaming.catshelper.{BracketThrowable, Log, MeasureDuration, MonadThrowable}
import com.evolution.kafka.journal.*
import com.evolution.kafka.journal.util.CatsHelper.*
import com.evolution.kafka.journal.util.Fail
import com.evolution.kafka.journal.util.SkafkaHelper.*
import com.evolutiongaming.skafka.{Offset, Partition, Topic}
import org.scalatest.Assertion
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import play.api.libs.json.Json

import java.time.Instant
import scala.collection.immutable.SortedSet
import scala.util.Try

trait EventualJournalSpec extends AnyWordSpec with Matchers {
  import EventualJournalSpec.*

  def test[F[_]: BracketThrowable: Fail](withJournals: (EventualAndReplicated[F] => F[Assertion]) => F[Assertion])
    : Unit = {

    val withJournals1 = (key: Key, timestamp: Instant) => { (f: (Eventual[F], Replicated[F]) => F[Assertion]) =>
      {
        withJournals { journals =>
          implicit val log: Log[F] = Log.empty[F]
          implicit val clock: Clock[F] = Clock.const[F](nanos = 0, millis = 0)
          implicit val measureDuration: MeasureDuration[F] = MeasureDuration.fromClock(clock)
          val eventual = {
            val journal = journals
              .eventual
              .withLog(log)
              .withMetrics(EventualJournal.Metrics.empty[F])
            Eventual[F](journal, key)
          }
          val replicated = {
            val journal = journals
              .replicated
              .withLog(log)
              .enhanceError
              .withMetrics(ReplicatedJournal.Metrics.empty[F])
              .toFlat
            Replicated[F](journal, key, timestamp)
          }
          f(eventual, replicated)
        }
      }
    }

    test1(withJournals1)
  }

  private def test1[F[_]: MonadThrowable: Fail](
    withJournals: (Key, Instant) => ((Eventual[F], Replicated[F]) => F[Assertion]) => F[Assertion],
  ): Unit = {

    implicit val monoidUnit: Monoid[F[Unit]] = Applicative.monoid[F, Unit]

    val key = EventualJournalSpec.key

    def withJournals1(f: (Eventual[F], Replicated[F]) => F[Assertion]): F[Assertion] = {
      withJournals(key, timestamp) {
        case (eventual, replicated) =>
          f(eventual, replicated)
      }
    }

    def eventOf(pointer: JournalPointer): EventRecord[EventualPayloadAndType] = {
      val event = Event[EventualPayloadAndType](pointer.seqNr)
      val headers = Headers(("key", "value"))
      EventRecord(
        event = event,
        timestamp = timestamp,
        partitionOffset = pointer.partitionOffset,
        metadata = RecordMetadata(HeaderMetadata(Json.obj(("key", "value")).some), PayloadMetadata.empty),
        headers = headers,
        origin = none,
        version = none,
      )
    }

    for {
      seqNr <- List(SeqNr.min, SeqNr.unsafe(5), SeqNr.unsafe(10))
      size <- List(0, 1, 2, 5, 10)
      batch <- List(true, false)
    } {
      val name = s"seqNr: $seqNr, events: $size, batch: $batch"

      def eventsOf(from: JournalPointer, size: Int) = {
        for {
          n <- (0 until size).toList
          seqNr <- SeqNr.opt(from.seqNr.value + n).toList
          offset = from.offset.value + n
          pointer = journalPointerOf(offset, seqNr)
        } yield {
          eventOf(pointer)
        }
      }

      val events = eventsOf(journalPointerOf(Offset.min.value, seqNr), size)
      val pointers = events.map(_.pointer)
      val pointerLast = pointers.lastOption
      val partitionOffsetNext = pointerLast.fold(partitionOffsetOf(Offset.min))(_.partitionOffset.next)
      val seqNrsAll = {
        val end = pointerLast.fold(seqNr)(_.seqNr)
        val seqNrs = (seqNr to end).toNel
        (SeqNr.min :: SeqNr.max :: seqNrs).distinct
      }

      def withJournals2(f: (Eventual[F], Replicated[F]) => F[Assertion]): F[Assertion] = {
        withJournals1 {
          case (eventual, replicated) =>
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
        withJournals2 {
          case (eventual, _) =>
            for {
              a <- eventual.events()
            } yield {
              a shouldEqual events
            }
        }
      }

      for {
        seqNrLast <- pointerLast
        from <- seqNrLast.next
        size <- List(1, 5)
      } {
        s"append $size to existing journal, $name" in {
          withJournals2 {
            case (eventual, replicated) =>
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
        deleteTo = pointer.seqNr.toDeleteTo
      } {
        s"delete to $deleteTo, $name" in {
          withJournals2 {
            case (eventual, replicated) =>
              for {
                _ <- replicated.delete(deleteTo, partitionOffsetNext)
                expected = events.dropWhile(_.seqNr <= deleteTo.value)
                a <- eventual.events()
                _ = a shouldEqual expected
                a <- eventual.pointer
              } yield {
                a shouldEqual pointerLast.map(_.withPartitionOffset(partitionOffsetNext))
              }
          }
        }

        s"delete to $deleteTo and then append, $name" in {
          withJournals2 {
            case (eventual, replicated) =>
              for {
                _ <- replicated.delete(deleteTo, partitionOffsetNext)
                expected = events.dropWhile(_.seqNr <= deleteTo.value)
                a <- eventual.events()
                _ = a shouldEqual expected
                pointer <- eventual.pointer
                _ = pointer shouldEqual pointerLast.map(_.withPartitionOffset(partitionOffsetNext))
                event = {
                  val seqNr = for {
                    seqNr <- pointerLast
                    seqNr <- seqNr.next
                  } yield seqNr
                  eventOf(seqNr getOrElse JournalPointer.min)
                }
                pointer <- pointer.next.getOrError[F]("pointer.next")
                event <- event.copy(partitionOffset = pointer.partitionOffset).pure[F]
                _ <- replicated.append(Nel.of(event))
                a <- eventual.events()
                _ = a shouldEqual expected ++ List(event)
                a <- eventual.pointer
              } yield {
                a shouldEqual event.pointer.some
              }
          }
        }
      }

      for {
        pointer <- pointerLast
        deleteTo = pointer.seqNr.toDeleteTo
      } {
        s"deleteTo $deleteTo and then deleteTo $deleteTo, $name" in {
          withJournals2 {
            case (eventual, replicated) =>
              for {
                _ <- replicated.delete(deleteTo, partitionOffsetNext)
                a <- eventual.events()
                _ = a shouldEqual Nil
                a <- eventual.pointer
                _ = a shouldEqual JournalPointer(partitionOffsetNext, deleteTo.value).some
                partitionOffset2 = partitionOffsetNext.next
                _ <- replicated.delete(deleteTo, partitionOffset2)
                a <- eventual.events()
                _ = a shouldEqual Nil
                a <- eventual.pointer
              } yield {
                a shouldEqual JournalPointer(partitionOffset2, deleteTo.value).some
              }
          }
        }
      }

      for {
        from <- seqNrsAll.toList
      } {
        s"read from: $from, $name" in {
          val expected = events.dropWhile(_.seqNr < from)

          withJournals2 {
            case (eventual, _) =>
              for {
                a <- eventual.events(from)
              } yield {
                a shouldEqual expected
              }
          }
        }
      }

      s"pointer, $name" in {
        withJournals2 {
          case (eventual, _) =>
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

      val partitionOffset = PartitionOffset(partition = Partition.unsafe(1), offset = Offset.unsafe(1))
      val topicPointers = TopicPointers(Map((partitionOffset.partition, partitionOffset.offset)))

      val name = s" topics: $size"

      val topics = for { idx <- 0 to size } yield s"topic-$idx"

      def withJournals3(f: (Eventual[F], Replicated[F]) => F[Assertion]): F[Assertion] = {
        withJournals1 {
          case (eventual, replicated) =>
            for {
              _ <- topics
                .toList
                .foldMapM { topic =>
                  replicated.offsetSave(topic, partitionOffset.partition, partitionOffset.offset)
                }
              result <- f(eventual, replicated)
            } yield result
        }
      }

      for {
        topic <- topics
        topicPointer <- topicPointers.values
      } {
        s"save pointer, $name, topic: $topic" in {
          withJournals3 {
            case (_, replicated) =>
              val (partition, offset) = topicPointer
              for {
                a <- replicated.offset(topic, partition)
              } yield {
                a shouldEqual offset.some
              }
          }
        }
      }

      s"topics, $name" in {
        withJournals3 {
          case (_, replicated) =>
            for {
              a <- replicated.topics
            } yield {
              a shouldEqual topics.toSortedSet
            }
        }
      }

      for {
        topic <- topics
        topicPointer <- topicPointers.values
      } {
        s"read pointer for $topic, $name " in {
          withJournals3 {
            case (eventual, replicated) =>
              val (partition, offset) = topicPointer
              for {
                a <- replicated.offset(topic, partition)
                _ = a shouldEqual offset.some
                a <- eventual.offset(topic, partition)
              } yield {
                a shouldEqual offset.some
              }
          }
        }
      }

      s"read pointers for unknown, $name " in {
        withJournals3 {
          case (eventual, replicated) =>
            for {
              a <- replicated.offset("unknown", partitionOffset.partition)
              _ = a shouldEqual none
              a <- eventual.offset("unknown", partitionOffset.partition)
            } yield {
              a shouldEqual none
            }
        }
      }
    }

    for {
      deleteTo <- List(1, 2, 5)
      deleteTo <- SeqNr.opt(deleteTo.toLong)
      deleteTo <- deleteTo.toDeleteTo.some
      deleteToPointer = journalPointerOf(offset = 1, seqNr = deleteTo.value)
    } {
      s"deleteTo $deleteTo on empty journal" in {
        withJournals1 {
          case (eventual, replicated) =>
            for {
              _ <- replicated.delete(deleteTo, deleteToPointer.partitionOffset)
              a <- eventual.events()
              _ = a shouldEqual Nil
              a <- eventual.pointer
            } yield {
              a shouldEqual deleteToPointer.some
            }
        }
      }

      s"deleteTo $deleteTo on empty journal and then append" in {
        withJournals1 {
          case (eventual, replicated) =>
            for {
              _ <- replicated.delete(deleteTo, deleteToPointer.partitionOffset)
              a <- eventual.events()
              _ = a shouldEqual Nil
              event = {
                val pointerNext = deleteToPointer.next getOrElse JournalPointer.min
                eventOf(pointerNext)
              }
              _ <- replicated.append(Nel.of(event))
              a <- eventual.events()
              _ = a shouldEqual List(event)
              a <- eventual.pointer
            } yield {
              a shouldEqual event.pointer.some
            }
        }
      }
    }

    "delete, append, delete, append, append, delete" in {
      withJournals1 {
        case (eventual, replicated) =>
          for {
            a <- eventual.events()
            _ = a shouldEqual Nil
            a <- eventual.pointer
            _ = a shouldEqual None
            event1 = eventOf(journalPointerOf(offset = 2, seqNr = SeqNr.unsafe(1)))
            _ <- replicated.append(Nel.of(event1))
            a <- eventual.events()
            _ = a shouldEqual List(event1)
            a <- eventual.pointer
            _ = a shouldEqual event1.pointer.some
            event2 = eventOf(journalPointerOf(offset = 3, seqNr = SeqNr.unsafe(2)))
            _ <- replicated.append(partitionOffsetOf(Offset.unsafe(4)), Nel.of(event2))
            a <- eventual.events()
            _ = a shouldEqual List(event1, event2)
            a <- eventual.pointer
            _ = a shouldEqual journalPointerOf(offset = 4, seqNr = event2.seqNr).some
            _ <- replicated.delete(event1.seqNr.toDeleteTo, partitionOffsetOf(Offset.unsafe(5)))
            a <- eventual.events()
            _ = a shouldEqual List(event2)
            a <- eventual.pointer
            _ = a shouldEqual journalPointerOf(offset = 5, seqNr = event2.seqNr).some
            event3 = eventOf(journalPointerOf(offset = 6, seqNr = SeqNr.unsafe(3)))
            _ <- replicated.append(Nel.of(event3))
            a <- eventual.events()
            _ = a shouldEqual List(event2, event3)
            a <- eventual.pointer
            _ = a shouldEqual event3.pointer.some
            _ <- replicated.delete(event3.seqNr.toDeleteTo, partitionOffsetOf(Offset.unsafe(7)))
            a <- eventual.events()
            _ = a shouldEqual Nil
            a <- eventual.pointer
            _ = a shouldEqual journalPointerOf(offset = 7, seqNr = event3.seqNr).some
            _ <- replicated.delete(SeqNr.max.toDeleteTo, partitionOffsetOf(Offset.unsafe(8)))
            a <- eventual.events()
            _ = a shouldEqual Nil
            a <- eventual.pointer
          } yield {
            a shouldEqual journalPointerOf(offset = 8, seqNr = event3.seqNr).some
          }
      }
    }

    "append not from beginning" in {
      val events = Nel.of(
        eventOf(journalPointerOf(offset = 10, seqNr = SeqNr.unsafe(5))),
        eventOf(journalPointerOf(offset = 10, seqNr = SeqNr.unsafe(6))),
        eventOf(journalPointerOf(offset = 10, seqNr = SeqNr.unsafe(7))),
      )

      withJournals1 {
        case (eventual, replicated) =>
          for {
            _ <- replicated.append(events)
            a <- eventual.pointer
            _ = a shouldEqual events.last.pointer.some
            a <- eventual.events(SeqNr.min)
          } yield {
            a shouldEqual events.toList
          }
      }
    }

    "ids" in {
      withJournals1 {
        case (eventual, replicated) =>
          for {
            ids <- eventual.ids(topic)
            _ = ids shouldEqual List.empty
            event = eventOf(journalPointerOf(offset = 2, seqNr = SeqNr.unsafe(1)))
            _ <- replicated.append(Nel.of(event))
            ids <- eventual.ids(topic)
            _ = ids shouldEqual List(key.id)
            ids <- eventual.ids("unknown")
          } yield {
            ids shouldEqual List.empty
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

  def journalPointerOf(offset: Long, seqNr: SeqNr): JournalPointer = {
    JournalPointer(partitionOffsetOf(Offset.unsafe(offset)), seqNr)
  }

  trait Eventual[F[_]] {

    def events(from: SeqNr = SeqNr.min): F[List[EventRecord[EventualPayloadAndType]]]

    def pointer: F[Option[JournalPointer]]

    def offset(topic: Topic, partition: Partition): F[Option[Offset]]

    def ids(topic: Topic): F[List[String]]
  }

  object Eventual {

    def apply[F[_]: Monad](journal: EventualJournal[F], key: Key): Eventual[F] = new Eventual[F] {

      def events(from: SeqNr = SeqNr.min) = {
        journal.read(key, from).toList
      }

      def pointer = journal.pointer(key)

      def offset(topic: Topic, partition: Partition): F[Option[Offset]] = {
        journal.offset(topic, partition)
      }

      def ids(topic: Topic) = journal.ids(topic).toList.map { _.sorted }
    }
  }

  trait Replicated[F[_]] {

    def topics: F[SortedSet[Topic]]

    final def append(events: Nel[EventRecord[EventualPayloadAndType]]): F[Unit] = {
      val partitionOffset = events.last.partitionOffset // TODO add test for custom offset
      append(partitionOffset, events)
    }

    def append(partitionOffset: PartitionOffset, events: Nel[EventRecord[EventualPayloadAndType]]): F[Unit]

    def delete(deleteTo: DeleteTo, partitionOffset: PartitionOffset): F[Unit]

    def offset(topic: Topic, partition: Partition): F[Option[Offset]]

    def offsetSave(topic: Topic, partition: Partition, offset: Offset): F[Unit]
  }

  object Replicated {

    def apply[F[_]: FlatMap](
      journal: ReplicatedJournalFlat[F],
      key: Key,
      timestamp: Instant,
    ): Replicated[F] = {
      new Replicated[F] {

        def topics = journal.topics

        def append(partitionOffset: PartitionOffset, events: Nel[EventRecord[EventualPayloadAndType]]) = {
          // TODO expiry: define expireAfter and test
          journal.append(key, partitionOffset.partition, partitionOffset.offset, timestamp, none, events).void
        }

        def delete(deleteTo: DeleteTo, partitionOffset: PartitionOffset) = {
          journal.delete(key, partitionOffset.partition, partitionOffset.offset, timestamp, deleteTo, None).void
        }

        def offset(topic: Topic, partition: Partition) = {
          journal.offset(topic, partition)
        }

        def offsetSave(topic: Topic, partition: Partition, offset: Offset) = {
          journal.offsetUpdate(topic, partition, offset, timestamp)
        }
      }
    }
  }

  final case class EventualAndReplicated[F[_]](eventual: EventualJournal[F], replicated: ReplicatedJournal[F])

  implicit class JournalPointerOps(val self: JournalPointer) extends AnyVal {

    def next: Option[JournalPointer] = {
      for {
        seqNr <- self.seqNr.next[Option]
      } yield {
        val partitionOffset = self.partitionOffset
        self.copy(seqNr = seqNr, partitionOffset = partitionOffset.copy(offset = partitionOffset.offset.inc[Try].get))
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

    def min: JournalPointer = journalPointerOf(Offset.min.value, SeqNr.min)
  }

  implicit class PartitionOffsetOps(val self: PartitionOffset) extends AnyVal {

    def next: PartitionOffset = self.copy(offset = self.offset.inc[Try].get)
  }
}
