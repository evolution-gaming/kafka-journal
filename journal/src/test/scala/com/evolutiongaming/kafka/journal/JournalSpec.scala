package com.evolutiongaming.kafka.journal

import java.time.Instant

import com.evolutiongaming.concurrent.CurrentThreadExecutionContext
import com.evolutiongaming.concurrent.async.AsyncConverters._
import com.evolutiongaming.kafka.journal.FoldWhileHelper._
import com.evolutiongaming.kafka.journal.SeqNr.Helper._
import com.evolutiongaming.kafka.journal.eventual.{EventualJournal, TopicPointers}
import com.evolutiongaming.nel.Nel
import com.evolutiongaming.safeakka.actor.ActorLog
import com.evolutiongaming.skafka.{Offset, Partition, Topic}
import org.scalatest.{Matchers, WordSpec}

import scala.collection.immutable.Queue
import scala.concurrent.{ExecutionContext, Future}

class JournalSpec extends WordSpec with Matchers {
  import JournalSpec._

  def test(createJournal: () => SeqNrJournal): Unit = {

    for {
      size <- 0 to 5
      seqNrs = (1L to size.toLong).toList.map(_.toSeqNr) // TODO convert to SeqRange
      combination <- Combinations(seqNrs)
    } {

      val seqNrLast = seqNrs.lastOption

      def createAndAppend() = {
        val journal = createJournal()
        for {
          seqNrs <- combination
        } journal.append(seqNrs.head, seqNrs.tail: _*)
        journal
      }

      val name = combination.map(_.mkString("[", ",", "]")).mkString(",")

      s"$name append" in {
        val journal = createAndAppend()
        journal.read(SeqRange.All) shouldEqual seqNrs
      }

      s"$name read" in {
        val journal = createAndAppend()
        journal.read(SeqRange.All) shouldEqual seqNrs
        val last = seqNrLast getOrElse SeqNr.Min
        journal.read(SeqNr.Min __ last) shouldEqual seqNrs
        journal.read(SeqNr.Min __ last + 1) shouldEqual seqNrs
      }

      s"$name delete all" in {
        val journal = createAndAppend()
        for {seqNr <- seqNrLast} journal.delete(seqNr)
        journal.read(SeqRange.All) shouldEqual Nil
        journal.lastSeqNr(SeqNr.Min) shouldEqual seqNrLast
      }

      s"$name delete SeqNr.Max" in {
        val journal = createAndAppend()
        journal.delete(SeqNr.Max)
        journal.read(SeqRange.All) shouldEqual Nil
        journal.lastSeqNr(SeqNr.Min) shouldEqual seqNrLast
      }

      s"$name delete SeqNr.Min" in {
        val journal = createAndAppend()
        journal.delete(SeqNr.Min)
        journal.read(SeqRange.All) shouldEqual seqNrs.dropWhile(_ <= SeqNr.Min)
        journal.lastSeqNr(SeqNr.Min) shouldEqual seqNrLast
      }

      s"$name lastSeqNr" in {
        val journal = createAndAppend()
        journal.lastSeqNr(SeqNr.Max) shouldEqual None
        journal.lastSeqNr(SeqNr.Min) shouldEqual seqNrLast
        journal.lastSeqNr(seqNrLast getOrElse SeqNr.Min) shouldEqual seqNrLast
      }

      for {
        _ <- seqNrLast
        seqNr <- seqNrs.tail.lastOption
      } {

        s"$name delete except last" in {
          val journal = createAndAppend()
          journal.delete(seqNr)
          journal.read(SeqRange.All) shouldEqual seqNrs.dropWhile(_ <= seqNr)
          journal.lastSeqNr(SeqNr.Min) shouldEqual seqNrLast
        }

        s"$name read tail" in {
          val journal = createAndAppend()
          journal.read(seqNr __ SeqNr.Max) shouldEqual seqNrs.dropWhile(_ < seqNr)
        }
      }
    }

    "read SeqNr.Max" in {
      val journal = createJournal()
      journal.read(SeqRange(SeqNr.Max)) shouldEqual Nil
      journal.append(1.toSeqNr)
      journal.read(SeqRange(SeqNr.Max)) shouldEqual Nil
    }

    "append, delete, append, delete, append, read, lastSeqNr" in {
      val journal = createJournal()
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
      journal.lastSeqNr(SeqNr.Min) shouldEqual Some(SeqNr(4))
    }
  }


  "Journal" when {

    "eventual journal is empty" should {

      test(() => journalOf())

      def journalOf() = {
        var actions: Queue[ActionRecord] = Queue.empty
        val eventualJournal = EventualJournal.Empty

        val withReadActions = WithReadActionsOneByOne(actions)

        val writeAction = new WriteAction {
          def apply(action: Action) = {
            val offset = actions.size.toLong + 1
            val record = ActionRecord(action, offset)
            actions = actions.enqueue(record)
            PartitionOffset(partition = partition, offset = offset).async
          }
        }
        SeqNrJournal(eventualJournal, withReadActions, writeAction)
      }
    }


    "kafka journal is empty" should {

      test(() => journalOf())

      def journalOf() = {
        var actions: Queue[ActionRecord] = Queue.empty
        var replicatedState = EventualJournalOf.State.Empty

        val eventualJournal = EventualJournalOf(replicatedState)

        val withReadActions = {
          def marks() = actions.collect { case action @ ActionRecord(_: Action.Mark, _) => action }

          WithReadActionsOneByOne(marks())
        }

        val writeAction = new WriteAction {

          def apply(action: Action) = {
            val offset = actions.size.toLong + 1
            val record = ActionRecord(action, offset)
            actions = actions.enqueue(record)
            replicatedState = replicatedState(record)
            PartitionOffset(partition = partition, offset = offset).async
          }
        }

        SeqNrJournal(eventualJournal, withReadActions, writeAction)
      }
    }


    "kafka and eventual journals are consistent" should {
      test(() => journalOf())

      def journalOf() = {
        var actions: Queue[ActionRecord] = Queue.empty
        var replicatedState = EventualJournalOf.State.Empty

        val eventualJournal = EventualJournalOf(replicatedState)

        val withReadActions = WithReadActionsOneByOne(actions)

        val writeAction = new WriteAction {

          def apply(action: Action) = {
            val offset = actions.size.toLong + 1
            val record = ActionRecord(action, offset)
            actions = actions.enqueue(record)
            replicatedState = replicatedState(record)
            PartitionOffset(partition = partition, offset = offset).async
          }
        }

        SeqNrJournal(eventualJournal, withReadActions, writeAction)
      }
    }

    "kafka and eventual journals are consistent, however eventual offset is behind" should {
      test(() => journalOf())

      def journalOf() = {
        var actions: Queue[ActionRecord] = Queue.empty
        var replicatedState = EventualJournalOf.State.Empty

        val eventualJournal = EventualJournalOf(replicatedState)

        val withReadActions = WithReadActionsOneByOne(actions)

        val writeAction = new WriteAction {

          def apply(action: Action) = {
            val offset = actions.size.toLong + 1
            val record = ActionRecord(action, offset)
            actions = actions.enqueue(record)
            replicatedState = replicatedState(record, offset - 2)

            PartitionOffset(partition = partition, offset = offset).async
          }
        }

        SeqNrJournal(eventualJournal, withReadActions, writeAction)
      }
    }

    for {
      n <- 1 to 4
    } {
      s"eventual journal is $n events behind the kafka journal" should {
        test(() => journalOf())

        def journalOf() = {
          var actions: Queue[ActionRecord] = Queue.empty
          var replicatedState = EventualJournalOf.State.Empty

          val eventualJournal = EventualJournalOf(replicatedState)

          val withReadActions = WithReadActionsOneByOne(actions)

          val writeAction = new WriteAction {

            def apply(action: Action) = {

              val offset = actions.size.toLong + 1
              val record = ActionRecord(action, offset)
              actions = actions.enqueue(record)

              for {
                actions <- actions.dropLast(n)
                action <- actions.lastOption
              } replicatedState = replicatedState(action)

              PartitionOffset(partition = partition, offset = offset).async
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
    def append(seqNr: SeqNr, seqNrs: SeqNr*): Unit
    def read(range: SeqRange): List[SeqNr]

    // TODO not sure this should be a part of this API
    def lastSeqNr(from: SeqNr): Option[SeqNr]
    def delete(to: SeqNr): Unit
  }

  object SeqNrJournal {

    def apply(journal: Journal): SeqNrJournal = {

      new SeqNrJournal {

        def append(seqNr: SeqNr, seqNrs: SeqNr*) = {
          val events = for {seqNr <- Nel(seqNr, seqNrs: _*)} yield Event(seqNr)
          journal.append(events, timestamp).get()
        }

        def read(range: SeqRange) = {
          val result = {
            val result = journal.foldWhile(range.from, List.empty[SeqNr]) { (seqNrs, event) =>
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

        def lastSeqNr(from: SeqNr) = journal.lastSeqNr(from).get()

        def delete(to: SeqNr) = journal.delete(to, timestamp).get()
      }
    }

    def apply(
      eventual: EventualJournal,
      withReadActions: WithReadActions,
      writeAction: WriteAction): SeqNrJournal = {

      val journal = Journal(key, ActorLog.empty, eventual, withReadActions, writeAction)
      SeqNrJournal(journal)
    }
  }

  // TODO implement via mocking EventualCassandra
  object EventualJournalOf {

    def apply(state: => State): EventualJournal = {

      new EventualJournal {

        def topicPointers(topic: Topic) = {
          val pointers = Map(partition -> state.offset)
          TopicPointers(pointers).async
        }

        def foldWhile[S](key: Key, from: SeqNr, s: S)(f: Fold[S, ReplicatedEvent]) = {

          def read(state: State) = {
            state.events.foldWhile(s) { (s, replicated) =>
              val seqNr = replicated.event.seqNr
              if (seqNr >= from) f(s, replicated)
              else s.continue
            }
          }

          read(state).async
        }

        def lastSeqNr(key: Key, from: SeqNr) = {

          def lastSeqNr(state: State) = {
            val seqNr = state.events.lastOption.map(_.event.seqNr)
            val lastSeqNr = (seqNr max state.deleteTo).filter(_ >= from)
            lastSeqNr.async
          }

          lastSeqNr(state)
        }
      }
    }


    case class State(
      events: Queue[ReplicatedEvent] = Queue.empty,
      deleteTo: Option[SeqNr] = None,
      offset: Offset = 0l) {

      def apply(record: ActionRecord): State = {
        apply(record, record.offset)
      }

      def apply(record: ActionRecord, offset: Offset): State = {

        def onAppend(action: Action.Append) = {
          val batch = for {
            event <- EventsSerializer.fromBytes(action.events)
          } yield {
            val partitionOffset = PartitionOffset(partition, record.offset)
            ReplicatedEvent(event, timestamp, partitionOffset)
          }
          copy(events = events.enqueue(batch.toList), offset = offset)
        }

        def onDelete(action: Action.Delete) = {
          events.lastOption.fold(this) { last =>
            val lastSeqNr = last.event.seqNr
            if (lastSeqNr <= action.to) copy(events = Queue.empty, deleteTo = Some(lastSeqNr), offset)
            else {
              val left = events.dropWhile(_.event.seqNr <= action.to)
              copy(events = left, deleteTo = Some(action.to), offset)
            }
          }
        }

        record.action match {
          case action: Action.Append => onAppend(action)
          case action: Action.Delete => onDelete(action)
          case action: Action.Mark   => copy(offset = offset)
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