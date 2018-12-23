package com.evolutiongaming.kafka.journal

import java.time.Instant
import java.util.concurrent.TimeUnit

import cats.effect.Clock
import cats.implicits._
import com.evolutiongaming.concurrent.CurrentThreadExecutionContext
import com.evolutiongaming.concurrent.async.Async
import com.evolutiongaming.concurrent.async.AsyncConverters._
import com.evolutiongaming.kafka.journal.AsyncHelper._
import com.evolutiongaming.kafka.journal.EventsSerializer._
import com.evolutiongaming.kafka.journal.FoldWhile._
import com.evolutiongaming.kafka.journal.SeqNr.ops._
import com.evolutiongaming.kafka.journal.eventual.{EventualJournal, TopicPointers}
import com.evolutiongaming.nel.Nel
import com.evolutiongaming.safeakka.actor.ActorLog
import com.evolutiongaming.skafka.{Offset, Partition, Topic}
import org.scalatest.{Matchers, WordSpec}

import scala.collection.immutable.Queue
import scala.concurrent.duration.TimeUnit
import scala.concurrent.{ExecutionContext, Future}

class JournalSpec extends WordSpec with Matchers {
  import JournalSpec._

  // TODO add test when Kafka missing it's tail comparing to eventual
  def test(journalOf: () => SeqNrJournal): Unit = {

    for {
      size <- 0 to 5
      seqNrs = (1L to size.toLong).toList.map(_.toSeqNr) // TODO convert to SeqRange
      combination <- Combinations(seqNrs)
    } {

      val seqNrLast = seqNrs.lastOption

      def createAndAppend(): (SeqNrJournal, Option[Offset]) = {
        val journal = journalOf()
        val offset = combination.foldLeft(Option.empty[Offset]) { (_, seqNrs) =>
          val offset = journal.append(seqNrs.head, seqNrs.tail: _*)
          Some(offset)
        }
        val offsetNext = offset.map(_ + 1)
        (journal, offsetNext)
      }

      val name = combination.map(_.mkString("[", ",", "]")).mkString(",")

      s"append, $name" in {
        val (journal, offset) = createAndAppend()
        journal.read(SeqRange.All) shouldEqual seqNrs
      }

      s"read, $name" in {
        val (journal, offset) = createAndAppend()
        journal.read(SeqRange.All) shouldEqual seqNrs
        val last = seqNrLast getOrElse SeqNr.Min
        journal.read(SeqNr.Min to last) shouldEqual seqNrs
        journal.read(SeqNr.Min to last.next.getOrElse(last)) shouldEqual seqNrs
      }

      s"delete all, $name" in {
        val (journal, offset) = createAndAppend()
        for {seqNr <- seqNrLast} journal.delete(seqNr)
        journal.read(SeqRange.All) shouldEqual Nil
        journal.lastSeqNr() shouldEqual seqNrLast
      }

      s"delete SeqNr.Max, $name" in {
        val (journal, offset) = createAndAppend()
        journal.delete(SeqNr.Max)
        journal.read(SeqRange.All) shouldEqual Nil
        journal.lastSeqNr() shouldEqual seqNrLast
      }

      s"delete SeqNr.Min, $name" in {
        val (journal, offset) = createAndAppend()
        journal.delete(SeqNr.Min) shouldEqual offset.map(_ + 1)
        journal.read(SeqRange.All) shouldEqual seqNrs.dropWhile(_ <= SeqNr.Min)
        journal.lastSeqNr() shouldEqual seqNrLast
      }

      s"lastSeqNr, $name" in {
        val (journal, offset) = createAndAppend()
        journal.lastSeqNr() shouldEqual seqNrLast
      }

      for {
        _ <- seqNrLast
        seqNr <- seqNrs.tail.lastOption
      } {

        s"delete except last, $name" in {
          val (journal, offset) = createAndAppend()
          journal.delete(seqNr)
          journal.read(SeqRange.All) shouldEqual seqNrs.dropWhile(_ <= seqNr)
          journal.lastSeqNr() shouldEqual seqNrLast
        }

        s"read tail, $name" in {
          val (journal, offset) = createAndAppend()
          journal.read(seqNr to SeqNr.Max) shouldEqual seqNrs.dropWhile(_ < seqNr)
        }
      }
    }

    "read SeqNr.Max" in {
      val journal = journalOf()
      journal.read(SeqRange(SeqNr.Max)) shouldEqual Nil
      journal.append(1.toSeqNr)
      journal.read(SeqRange(SeqNr.Max)) shouldEqual Nil
    }

    "append, delete, append, delete, append, read, lastSeqNr" in {
      val journal = journalOf()
      journal.append(1.toSeqNr)
      journal.delete(3.toSeqNr)
      journal.append(2.toSeqNr, 3.toSeqNr)
      journal.delete(2.toSeqNr)
      journal.append(4.toSeqNr)
      journal.read(SeqRange(1, 2)) shouldEqual Nil
      journal.read(SeqRange(2, 3)) shouldEqual List(3.toSeqNr)
      journal.read(SeqRange(3, 4)) shouldEqual List(3.toSeqNr, 4.toSeqNr)
      journal.read(SeqRange(4, 5)) shouldEqual List(4.toSeqNr)
      journal.read(SeqRange(5, 6)) shouldEqual Nil
      journal.lastSeqNr() shouldEqual Some(SeqNr(4))
    }
  }


  "Journal" when {

    "eventual journal is empty" should {

      test(() => journalOf())

      def journalOf() = {
        var actions: Queue[ActionRecord[Action]] = Queue.empty
        val eventualJournal = EventualJournal.empty[Async]

        val withReadActions = WithReadActionsOneByOne(actions)

        val writeAction = new AppendAction[Async] {
          def apply(action: Action) = {
            val offset = actions.size.toLong
            val partitionOffset = PartitionOffset(partition = partition, offset = offset)
            val record = ActionRecord(action, partitionOffset)
            actions = actions.enqueue(record)
            partitionOffset.async
          }
        }
        SeqNrJournal(eventualJournal, withReadActions, writeAction)
      }
    }


    "kafka journal is empty" should {

      test(() => journalOf())

      def journalOf() = {
        var actions: Queue[ActionRecord[Action]] = Queue.empty
        var replicatedState = EventualJournalOf.State.Empty

        val eventualJournal = EventualJournalOf(replicatedState)

        val withReadActions = {
          def marks() = actions.collect { case action @ ActionRecord(_: Action.Mark, _) => action }

          WithReadActionsOneByOne(marks())
        }

        val writeAction = new AppendAction[Async] {

          def apply(action: Action) = {
            val offset = actions.size.toLong
            val partitionOffset = PartitionOffset(partition = partition, offset = offset)
            val record = ActionRecord(action, partitionOffset)
            actions = actions.enqueue(record)
            replicatedState = replicatedState(record)
            partitionOffset.async
          }
        }

        SeqNrJournal(eventualJournal, withReadActions, writeAction)
      }
    }


    "kafka and eventual journals are consistent" should {
      test(() => journalOf())

      def journalOf() = {
        var actions: Queue[ActionRecord[Action]] = Queue.empty
        var replicatedState = EventualJournalOf.State.Empty

        val eventualJournal = EventualJournalOf(replicatedState)

        val withReadActions = WithReadActionsOneByOne(actions)

        val writeAction = new AppendAction[Async] {

          def apply(action: Action) = {
            val offset = actions.size.toLong
            val partitionOffset = PartitionOffset(partition = partition, offset = offset)
            val record = ActionRecord(action, partitionOffset)
            actions = actions.enqueue(record)
            replicatedState = replicatedState(record)
            partitionOffset.async
          }
        }

        SeqNrJournal(eventualJournal, withReadActions, writeAction)
      }
    }

    for {
      n <- 1 to 3
    } {
      s"kafka and eventual journals are consistent, however eventual offset is $n behind" should {
        test(() => journalOf())

        def journalOf() = {
          var actions: Queue[ActionRecord[Action]] = Queue.empty
          var replicatedState = EventualJournalOf.State.Empty

          val eventualJournal = EventualJournalOf(replicatedState)

          val withReadActions = WithReadActionsOneByOne(actions)

          val writeAction = new AppendAction[Async] {

            def apply(action: Action) = {
              val offset = actions.size.toLong
              val partitionOffset = PartitionOffset(partition = partition, offset = offset)
              val record = ActionRecord(action, partitionOffset)
              actions = actions.enqueue(record)
              replicatedState = replicatedState(record, (offset - n) max 0l)
              partitionOffset.async
            }
          }

          SeqNrJournal(eventualJournal, withReadActions, writeAction)
        }
      }
    }

    for {
      n <- 1 to 4
    } {
      s"eventual journal is $n actions behind the kafka journal" should {
        test(() => journalOf())

        def journalOf() = {
          var actions: Queue[ActionRecord[Action]] = Queue.empty
          var replicatedState = EventualJournalOf.State.Empty

          val eventualJournal = EventualJournalOf(replicatedState)

          val withReadActions = WithReadActionsOneByOne(actions)

          val writeAction = new AppendAction[Async] {

            def apply(action: Action) = {

              val offset = actions.size.toLong
              val partitionOffset = PartitionOffset(partition = partition, offset = offset)
              val record = ActionRecord(action, partitionOffset)
              actions = actions.enqueue(record)

              for {
                actions <- actions.dropLast(n)
                action <- actions.lastOption
              } replicatedState = replicatedState(action)

              partitionOffset.async
            }
          }

          SeqNrJournal(eventualJournal, withReadActions, writeAction)
        }
      }
    }

    for {
      n <- 1 to 3
      nn = n + 1
    } {
      s"eventual journal is $n actions behind and pointer is $nn behind the kafka journal" should {
        test(() => journalOf())

        def journalOf() = {
          var actions: Queue[ActionRecord[Action]] = Queue.empty
          var replicatedState = EventualJournalOf.State.Empty

          val eventualJournal = EventualJournalOf(replicatedState)

          val withReadActions = WithReadActionsOneByOne(actions)

          val writeAction = new AppendAction[Async] {

            def apply(action: Action) = {

              val offset = actions.size.toLong
              val partitionOffset = PartitionOffset(partition = partition, offset = offset)
              val record = ActionRecord(action, partitionOffset)
              actions = actions.enqueue(record)

              for {
                actions <- actions.dropLast(n)
                action <- actions.lastOption
              } replicatedState = replicatedState(action, (offset - n) max 0l)

              partitionOffset.async
            }
          }

          SeqNrJournal(eventualJournal, withReadActions, writeAction)
        }
      }
    }
  }
}

object JournalSpec {
  val key = Key(topic = "topic", id = "id")
  val timestamp = Instant.now()
  val partition: Partition = 0

  implicit val ec: ExecutionContext = CurrentThreadExecutionContext


  trait SeqNrJournal {

    def append(seqNr: SeqNr, seqNrs: SeqNr*): Offset

    def read(range: SeqRange): List[SeqNr]

    def lastSeqNr(): Option[SeqNr]

    def delete(to: SeqNr): Option[Offset]
  }

  object SeqNrJournal {

    def apply(journal: Journal[Async]): SeqNrJournal = {

      new SeqNrJournal {

        def append(seqNr: SeqNr, seqNrs: SeqNr*) = {
          val events = for {seqNr <- Nel(seqNr, seqNrs: _*)} yield Event(seqNr)
          journal.append(key, events, timestamp).get().offset
        }

        def read(range: SeqRange) = {
          val result = {
            val result = journal.read(key, range.from, List.empty[SeqNr]) { (seqNrs, event) =>
              val continue = event.seqNr <= range.to
              val result = {
                if (event.seqNr >= range.from && continue) event.seqNr :: seqNrs
                else seqNrs
              }
              result.switch(continue)
            }
            for {events <- result} yield events.reverse
          }
          result.get()
        }

        def lastSeqNr() = journal.pointer(key).get()

        def delete(to: SeqNr) = {
          journal.delete(key, to, timestamp).get().map(_.offset)
        }
      }
    }

    def apply(
      eventual: EventualJournal[Async],
      withReadActions: WithReadActions[Async],
      writeAction: AppendAction[Async]): SeqNrJournal = {

      val journal = Journal(ActorLog.empty, None, eventual, withReadActions, writeAction, HeadCache.empty[Async])
      implicit val log = Log.empty[Async]
      val withLogging = Journal[Async](journal)
      val withMetrics = Journal(withLogging, Journal.Metrics.empty(Async.unit))
      SeqNrJournal(withMetrics)
    }
  }

  // TODO implement via mocking EventualCassandra
  object EventualJournalOf {

    def apply(state: => State): EventualJournal[Async] = new EventualJournal[Async] {

      def pointers(topic: Topic) = {
        val pointers = state.offset.fold(TopicPointers.Empty) { offset =>
          val pointers = Map((partition, offset))
          TopicPointers(pointers)
        }
        pointers.async
      }

      def read[S](key: Key, from: SeqNr, s: S)(f: Fold[S, ReplicatedEvent]) = {

        def read(state: State) = {
          state.events.foldWhile(s) { (s, replicated) =>
            val seqNr = replicated.event.seqNr
            if (seqNr >= from) f(s, replicated)
            else s.continue
          }
        }

        read(state).async
      }

      def pointer(key: Key) = {

        def pointer(state: State) = {
          val seqNr = state.events.lastOption.map(_.event.seqNr)
          for {
            seqNr <- seqNr max state.deleteTo
            offset <- state.offset
          } yield {
            val partitionOffset = PartitionOffset(partition, offset)
            Pointer(partitionOffset, seqNr)
          }
        }

        pointer(state).async
      }
    }


    final case class State(
      events: Queue[ReplicatedEvent] = Queue.empty,
      deleteTo: Option[SeqNr] = None,
      offset: Option[Offset] = None) {

      def apply(record: ActionRecord[Action]): State = {
        apply(record, record.offset)
      }

      def apply(record: ActionRecord[Action], offset: Offset): State = {

        def updateOffset = copy(offset = Some(offset))

        def onAppend(action: Action.Append) = {
          val batch = for {
            event <- EventsFromPayload(action.payload, action.payloadType)
          } yield {
            val partitionOffset = PartitionOffset(partition, record.offset)
            ReplicatedEvent(event, timestamp, partitionOffset, None)
          }
          copy(events = events.enqueue(batch.toList), offset = Some(offset))
        }

        def onDelete(action: Action.Delete) = {
          events.lastOption.fold(updateOffset) { last =>
            val lastSeqNr = last.event.seqNr
            if (lastSeqNr <= action.to) {
              copy(
                events = Queue.empty,
                deleteTo = Some(lastSeqNr),
                offset = Some(offset))
            } else {
              val left = events.dropWhile(_.event.seqNr <= action.to)
              copy(
                events = left,
                deleteTo = Some(action.to),
                offset = Some(offset))
            }
          }
        }

        record.action match {
          case action: Action.Append => onAppend(action)
          case action: Action.Delete => onDelete(action)
          case action: Action.Mark   => updateOffset
        }
      }
    }

    object State {
      val Empty: State = State()
    }
  }


  implicit class TestFutureOps[T](val self: Future[T]) extends AnyVal {
    def get(): T = self.value.get.get
  }

  implicit class QueueOps[T](val self: Queue[T]) extends AnyVal {
    def dropLast(n: Int): Option[Queue[T]] = {
      if (self.size <= n) None
      else Some(self.dropRight(n))
    }
  }
}