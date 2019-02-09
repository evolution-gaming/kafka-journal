package com.evolutiongaming.kafka.journal

import java.time.Instant

import cats.Monad
import cats.data.StateT
import cats.implicits._
import com.evolutiongaming.concurrent.CurrentThreadExecutionContext
import com.evolutiongaming.kafka.journal.EventsSerializer._
import com.evolutiongaming.kafka.journal.SeqNr.ops._
import com.evolutiongaming.kafka.journal.eventual.{EventualJournal, TopicPointers}
import com.evolutiongaming.kafka.journal.stream.Stream
import com.evolutiongaming.kafka.journal.util.ConcurrentOf
import com.evolutiongaming.nel.Nel
import com.evolutiongaming.skafka.{Offset, Partition, Topic}
import org.scalatest.{Assertion, Matchers, WordSpec}

import scala.collection.immutable.Queue
import scala.concurrent.{ExecutionContext, Future}

class JournalSpec extends WordSpec with Matchers {
  import JournalSpec._

  // TODO add test when Kafka missing it's tail comparing to eventual
  def testF[F[_] : Monad](withJournal: (SeqNrJournal[F] => F[Assertion]) => Assertion): Unit = {
    for {
      size        <- 0 to 5
      seqNrs       = (1L to size.toLong).toList.map(_.toSeqNr) // TODO convert to SeqRange
      combination <- Combinations(seqNrs)
    } {

      val seqNrLast = seqNrs.lastOption

      def createAndAppend(f: (SeqNrJournal[F], Option[Offset]) => F[Assertion]) = {
        withJournal { journal =>
          for {
            offset <- combination.foldLeft(none[Offset].pure[F]) { (offset, seqNrs) =>
              for {
                _      <- offset
                offset <- journal.append(seqNrs.head, seqNrs.tail: _*)
              } yield {
                offset.some
              }
            }
            offsetNext  = offset.map(_ + 1)
            result     <- f(journal, offsetNext)
          } yield result
        }
      }

      val name = combination.map(_.mkString("[", ",", "]")).mkString(",")

      s"append, $name" in {
        createAndAppend { case (journal, _) =>
          for {
            a <- journal.read(SeqRange.All)
          } yield {
            a shouldEqual seqNrs
          }
        }
      }

      s"read, $name" in {
        createAndAppend { case (journal, _) =>
          for {
            a   <- journal.read(SeqRange.All)
            _    = a shouldEqual seqNrs
            last = seqNrLast getOrElse SeqNr.Min
            a   <- journal.read(SeqNr.Min to last)
            _    = a shouldEqual seqNrs
            a   <- journal.read(SeqNr.Min to last.next.getOrElse(last))
          } yield {
            a shouldEqual seqNrs
          }
        }
      }

      s"delete all, $name" in {
        createAndAppend { case (journal, _) =>
          for {
            _ <- seqNrLast.fold(().pure[F]) { seqNr => journal.delete(seqNr).void }
            a <- journal.read(SeqRange.All)
            _  = a shouldEqual Nil
            a <- journal.pointer
          } yield {
            a shouldEqual seqNrLast
          }
        }
      }

      s"delete SeqNr.Max, $name" in {
        createAndAppend { case (journal, _) =>
          for {
            _ <- journal.delete(SeqNr.Max)
            a <- journal.read(SeqRange.All)
            _  = a shouldEqual Nil
            a <- journal.pointer
          } yield {
            a shouldEqual seqNrLast
          }
        }
      }

      s"delete SeqNr.Min, $name" in {
        createAndAppend { case (journal, offset) =>
          for {
            a <- journal.delete(SeqNr.Min)
            _  = a shouldEqual offset.map(_ + 1)
            a <- journal.read(SeqRange.All)
            _  = a shouldEqual seqNrs.dropWhile(_ <= SeqNr.Min)
            a <- journal.pointer
          } yield {
            a shouldEqual seqNrLast
          }
        }
      }

      s"lastSeqNr, $name" in {
        createAndAppend { case (journal, _) =>
          for {
            a <- journal.pointer
          } yield {
            a shouldEqual seqNrLast
          }
        }
      }

      for {
        _     <- seqNrLast
        seqNr <- seqNrs.tail.lastOption
      } {

        s"delete except last, $name" in {
          createAndAppend { case (journal, _) =>
            for {
              _      <- journal.delete(seqNr)
              seqNrs <- journal.read(SeqRange.All)
              _       = seqNrs shouldEqual seqNrs.dropWhile(_ <= seqNr)
              seqNr  <- journal.pointer
            } yield {
              seqNr shouldEqual seqNrLast
            }
          }
        }

        s"read tail, $name" in {
          createAndAppend { case (journal, _) =>
            for {
              seqNrs <- journal.read(seqNr to SeqNr.Max)
            } yield {
              seqNrs shouldEqual seqNrs.dropWhile(_ < seqNr)
            }
          }
        }
      }
    }

    "read SeqNr.Max" in {
      withJournal { journal =>
        for {
          seqNrs <- journal.read(SeqRange(SeqNr.Max))
          _       = seqNrs shouldEqual Nil
          _      <- journal.append(1.toSeqNr)
          seqNrs <- journal.read(SeqRange(SeqNr.Max))
        } yield {
          seqNrs shouldEqual Nil
        }
      }
    }

    "append, delete, append, delete, append, read, lastSeqNr" in {
      withJournal { journal =>
        for {
          _      <- journal.append(1.toSeqNr)
          _      <- journal.delete(3.toSeqNr)
          _      <- journal.append(2.toSeqNr, 3.toSeqNr)
          _      <- journal.delete(2.toSeqNr)
          _      <- journal.append(4.toSeqNr)
          seqNrs <- journal.read(SeqRange(1, 2))
          _       = seqNrs shouldEqual Nil
          seqNrs <- journal.read(SeqRange(2, 3))
          _       = seqNrs shouldEqual List(3.toSeqNr)
          seqNrs <- journal.read(SeqRange(3, 4))
          _       = seqNrs shouldEqual List(3.toSeqNr, 4.toSeqNr)
          seqNrs <- journal.read(SeqRange(4, 5))
          _       = seqNrs shouldEqual List(4.toSeqNr)
          seqNrs <- journal.read(SeqRange(5, 6))
          _       = seqNrs shouldEqual Nil
          seqNr  <- journal.pointer
        } yield {
          seqNr shouldEqual Some(SeqNr(4))
        }
      }
    }
  }


  def test(journal: SeqNrJournal[StateM]) = {
    testF[StateM] { f =>
      val (_, result) = f(journal).run(State.Empty)
      result
    }
  }


  "Journal" when {

    // TODO add case with failing head cache
    for {
      (headCacheStr, headCache) <- List(
        ("invalid", HeadCache.empty[StateM]),
        ("valid",   StateM.headCache))
    } {

      val name = s"headCache: $headCacheStr"

      s"eventual journal is empty, $name" should {
        val journal = SeqNrJournal(
          EventualJournal.empty[StateM],
          StateM.withPollActions,
          StateM.appendAction,
          headCache)

        test(journal)
      }


      s"kafka journal is empty, $name" should {

        val withPollActions = new WithPollActions[StateM] {

          def apply[A](key: Key, partition: Partition, offset: Option[Offset])(f: PollActions[StateM] => StateM[A]) = {
            StateM { state =>
              val records = offset
                .fold(state.records) { offset => state.records.dropWhile(_.offset <= offset) }
                .collect { case action @ ActionRecord(_: Action.Mark, _) => action }
              val state1 = state.copy(recordsToRead = records)
              f(StateM.pollActions).run(state1)
            }
          }
        }

        val journal = SeqNrJournal(
          StateM.eventualJournal,
          withPollActions,
          StateM.appendAction,
          headCache)

        test(journal)
      }


      s"kafka and eventual journals are consistent, $name" should {
        val journal = SeqNrJournal(
          StateM.eventualJournal,
          StateM.withPollActions,
          StateM.appendAction,
          headCache)

        test(journal)
      }

      for {
        n <- 1 to 3
      } {
        s"kafka and eventual journals are consistent, however eventual offset is $n behind, $name" should {
          val appendAction = new AppendAction[StateM] {

            def apply(action: Action) = {
              StateM { state =>
                val offset = state.records.size.toLong
                val partitionOffset = PartitionOffset(partition = partition, offset = offset)
                val record = ActionRecord(action, partitionOffset)
                val records = state.records.enqueue(record)

                val replicatedState = state.replicatedState(record, (offset - n) max 0l)
                val state1 = state.copy(records = records, replicatedState = replicatedState)
                (state1, partitionOffset)
              }
            }
          }

          val journal = SeqNrJournal(
            StateM.eventualJournal,
            StateM.withPollActions,
            appendAction,
            headCache)

          test(journal)
        }
      }

      for {
        n <- 1 to 4
      } {
        s"eventual journal is $n actions behind the kafka journal, $name" should {

          val appendAction = new AppendAction[StateM] {

            def apply(action: Action) = {
              StateM { state =>
                val offset = state.records.size.toLong
                val partitionOffset = PartitionOffset(partition = partition, offset = offset)
                val record = ActionRecord(action, partitionOffset)
                val records = state.records.enqueue(record)

                val replicatedState = for {
                  actions <- records.dropLast(n)
                  action <- actions.lastOption
                } yield state.replicatedState(action)
                val state1 = state.copy(records = records, replicatedState = replicatedState getOrElse state.replicatedState)
                (state1, partitionOffset)
              }
            }
          }

          val journal = SeqNrJournal(
            StateM.eventualJournal,
            StateM.withPollActions,
            appendAction,
            headCache)

          test(journal)
        }
      }

      for {
        n <- 1 to 3
        nn = n + 1
      } {
        s"eventual journal is $n actions behind and pointer is $nn behind the kafka journal, $name" should {

          val appendAction = new AppendAction[StateM] {

            def apply(action: Action) = {
              StateM { state =>
                val offset = state.records.size.toLong
                val partitionOffset = PartitionOffset(partition = partition, offset = offset)
                val record = ActionRecord(action, partitionOffset)
                val records = state.records.enqueue(record)

                val replicatedState = for {
                  actions <- records.dropLast(n)
                  action <- actions.lastOption
                } yield state.replicatedState(action, (offset - n) max 0l)
                val state1 = state.copy(records = records, replicatedState = replicatedState getOrElse state.replicatedState)
                (state1, partitionOffset)
              }
            }
          }

          val journal = SeqNrJournal(
            StateM.eventualJournal,
            StateM.withPollActions,
            appendAction,
            headCache)

          test(journal)
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


  trait SeqNrJournal[F[_]] {

    def append(seqNr: SeqNr, seqNrs: SeqNr*): F[Offset]

    def read(range: SeqRange): F[List[SeqNr]]

    def pointer: F[Option[SeqNr]]

    def delete(to: SeqNr): F[Option[Offset]]
  }

  object SeqNrJournal {

    def apply[F[_] : Monad](journal: Journal[F]): SeqNrJournal[F] = {

      new SeqNrJournal[F] {

        def append(seqNr: SeqNr, seqNrs: SeqNr*) = {
          val events = for {
            seqNr <- Nel(seqNr, seqNrs: _*)
          } yield {
            Event(seqNr)
          }
          for {
            partitionOffset <- journal.append(key, events)
          } yield {
            partitionOffset.offset
          }
        }

        def read(range: SeqRange) = {
          import Stream.Cmd
          val stream = journal
            .read(key, range.from)
            .mapCmd { event =>
              if (event.seqNr < range.from) Cmd.skip
              else if (event.seqNr <= range.to) Cmd.take(event.seqNr)
              else Cmd.stop
            }
          stream.toList
        }

        def pointer = journal.pointer(key)

        def delete(to: SeqNr) = {
          for {
            partitionOffset <- journal.delete(key, to)
          } yield for {
            partitionOffset <- partitionOffset
          } yield {
            partitionOffset.offset
          }
        }
      }
    }

    def apply[F[_] : Monad](
      eventual: EventualJournal[F],
      withPollActions: WithPollActions[F],
      writeAction: AppendAction[F],
      headCache: HeadCache[F]): SeqNrJournal[F] = {

      implicit val log = Log.empty[F]
      implicit val concurrent = ConcurrentOf.fromMonad[F]
      implicit val clock = ClockOf[F](timestamp.toEpochMilli)
      implicit val par = Par.sequential[F]
      val journal = Journal[F](None, eventual, withPollActions, writeAction, headCache)
        .withLog(log)
        .withMetrics(Journal.Metrics.empty[F])
      SeqNrJournal(journal)
    }
  }


  final case class State(
    records: Queue[ActionRecord[Action]] = Queue.empty,
    replicatedState: EventualJournalOf.State = EventualJournalOf.State.Empty,
    recordsToRead: Queue[ActionRecord[Action]] = Queue.empty)

  object State {
    val Empty: State = State()
  }


  type StateM[A] = StateT[cats.Id, State, A]

  object StateM {

    val eventualJournal: EventualJournal[StateM] = new EventualJournal[StateM] {

      def pointers(topic: Topic) = {
        StateM { state =>
          val topicPointers = state.replicatedState.offset.fold(TopicPointers.Empty) { offset =>
            val pointers = Map((partition, offset))
            TopicPointers(pointers)
          }

          (state, topicPointers)
        }
      }

      def read(key: Key, from: SeqNr) = {
        val events = StateM { state =>
          val events = state.replicatedState.events.toList.filter(_.seqNr >= from)
          (state, events)
        }

        for {
          events <- Stream.lift(events)
          event <- Stream[StateM].apply(events)
        } yield {
          event
        }
      }

      def pointer(key: Key) = {
        StateM { state =>

          val seqNr = state.replicatedState.events.lastOption.map(_.event.seqNr)
          val pointer = for {
            seqNr <- seqNr max state.replicatedState.deleteTo
            offset <- state.replicatedState.offset
          } yield {
            val partitionOffset = PartitionOffset(partition, offset)
            Pointer(partitionOffset, seqNr)
          }

          (state, pointer)
        }
      }
    }


    val pollActions: PollActions[StateM] = new PollActions[StateM] {
      def apply() = StateM { state =>
        state.recordsToRead.dequeueOption match {
          case Some((record, records)) => (state.copy(recordsToRead = records), List(record))
          case None                    => (state, Nil)
        }
      }
    }


    val withPollActions: WithPollActions[StateM] = new WithPollActions[StateM] {

      def apply[A](key: Key, partition: Partition, offset: Option[Offset])(f: PollActions[StateM] => StateM[A]) = {
        StateM { state =>
          val records = offset.fold(state.records) { offset => state.records.dropWhile(_.offset <= offset) }
          val state1 = state.copy(recordsToRead = records)
          f(pollActions).run(state1)
        }
      }
    }


    val appendAction: AppendAction[StateM] = new AppendAction[StateM] {

      def apply(action: Action) = {
        StateM { state =>
          val offset = state.records.size.toLong
          val partitionOffset = PartitionOffset(partition = partition, offset = offset)
          val record = ActionRecord(action, partitionOffset)
          val records = state.records.enqueue(record)

          val replicatedState = state.replicatedState(record)
          val state1 = state.copy(records = records, replicatedState = replicatedState)
          (state1, partitionOffset)
        }
      }
    }


    val headCache: HeadCache[StateM] = new HeadCache[StateM] {
      def get(key: Key, partition: Partition, offset: Offset) = {

        StateM { state =>
          val info = state.records.foldLeft(JournalInfo.empty) { (info, record) => info(record.action.header) }
          (state, HeadCache.Result.valid(info))
        }
      }
    }

    def apply[A](f: State => (State, A)): StateM[A] = StateT[cats.Id, State, A](f)
  }


  // TODO implement via mocking EventualCassandra
  object EventualJournalOf {

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
            ReplicatedEvent(event, timestamp, partitionOffset, None, action.header.metadata)
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
          case a: Action.Append => onAppend(a)
          case a: Action.Delete => onDelete(a)
          case _: Action.Mark   => updateOffset
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