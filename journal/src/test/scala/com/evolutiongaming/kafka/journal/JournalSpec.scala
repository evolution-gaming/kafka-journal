package com.evolutiongaming.kafka.journal

import java.time.Instant

import cats.data.{NonEmptyList => Nel}
import cats.effect.Clock
import cats.implicits._
import cats.{Id, Monad, Parallel}
import com.evolutiongaming.catshelper.ClockHelper._
import com.evolutiongaming.catshelper.{FromTry, Log}
import com.evolutiongaming.concurrent.CurrentThreadExecutionContext
import com.evolutiongaming.kafka.journal.conversions.{EventsToPayload, PayloadToEvents}
import com.evolutiongaming.kafka.journal.eventual.{EventualJournal, TopicPointers}
import com.evolutiongaming.kafka.journal.util.{ConcurrentOf, Fail}
import com.evolutiongaming.kafka.journal.util.SkafkaHelper._
import com.evolutiongaming.skafka.{Offset, Partition, Topic}
import com.evolutiongaming.smetrics.MeasureDuration
import com.evolutiongaming.sstream.Stream
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.{Assertion, Succeeded}

import scala.collection.immutable.Queue
import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try

class JournalSpec extends AnyWordSpec with Matchers {
  import JournalSpec._

  // TODO add test when Kafka missing it's tail comparing to eventual
  def testF[F[_] : Monad](withJournal: (SeqNrJournal[F] => F[Assertion]) => Assertion): Unit = {
    for {
      size        <- 0 to 5
      seqNrs       = (1 to size).toList.map { a => SeqNr.unsafe(a) } // TODO convert to SeqRange
      combination <- Combinations(seqNrs)
    } {

      val seqNrLast = seqNrs.lastOption

      def createAndAppend(f: (SeqNrJournal[F], Option[Offset]) => F[Assertion]) = {
        withJournal { journal =>
          
          def append(seqNrs: Nel[SeqNr]) = {
            for {
              offset <- journal.append(seqNrs.head, seqNrs.tail: _*)
            } yield {
              offset.some
            }
          }
          for {
            offset     <- combination.foldLeftM(none[Offset]) { (_, seqNrs) => append(seqNrs) }
            offsetNext  = offset.map { _.inc[Try].get }
            result     <- f(journal, offsetNext)
          } yield result
        }
      }

      val name = combination
        .map { _.toList.mkString("[", ",", "]") }
        .mkString(",")

      s"append, $name" in {
        createAndAppend { case (journal, _) =>
          for {
            a <- journal.read(SeqRange.all)
          } yield {
            a shouldEqual seqNrs
          }
        }
      }

      s"read, $name" in {
        createAndAppend { case (journal, _) =>
          for {
            a   <- journal.read(SeqRange.all)
            _    = a shouldEqual seqNrs
            last = seqNrLast getOrElse SeqNr.min
            a   <- journal.read(SeqNr.min to last)
            _    = a shouldEqual seqNrs
            a   <- journal.read(SeqNr.min to last.next[Option].getOrElse(last))
          } yield {
            a shouldEqual seqNrs
          }
        }
      }

      s"delete all, $name" in {
        createAndAppend { case (journal, _) =>
          for {
            _ <- seqNrLast.fold(().pure[F]) { seqNr => journal.delete(seqNr).void }
            a <- journal.read(SeqRange.all)
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
            _ <- journal.delete(SeqNr.max)
            a <- journal.read(SeqRange.all)
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
            a <- journal.delete(SeqNr.min)
            _  = a shouldEqual offset.map { _.inc[Try].get }
            a <- journal.read(SeqRange.all)
            _  = a shouldEqual seqNrs.dropWhile(_ <= SeqNr.min)
            a <- journal.pointer
            _  = a shouldEqual seqNrLast
          } yield Succeeded
        }
      }

      s"purge, $name" in {
        createAndAppend { case (journal, _) =>
          for {
            _ <- journal.purge
            a <- journal.read(SeqRange.all)
            _  = a shouldEqual List.empty
            a <- journal.pointer
            _  = a shouldEqual none
          } yield Succeeded
        }
      }

      s"lastSeqNr, $name" in {
        createAndAppend { case (journal, _) =>
          for {
            a <- journal.pointer
            _  = a shouldEqual seqNrLast
          } yield Succeeded
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
              seqNrs <- journal.read(SeqRange.all)
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
              seqNrs <- journal.read(seqNr to SeqNr.max)
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
          seqNrs <- journal.read(SeqRange(SeqNr.max))
          _       = seqNrs shouldEqual Nil
          _      <- journal.append(SeqNr.unsafe(1))
          seqNrs <- journal.read(SeqRange(SeqNr.max))
        } yield {
          seqNrs shouldEqual Nil
        }
      }
    }

    "append, delete, append, delete, append, read, lastSeqNr, purge" in {
      withJournal { journal =>
        for {
          _       <- journal.append(SeqNr.unsafe(1))
          _       <- journal.delete(SeqNr.unsafe(3))
          _       <- journal.append(SeqNr.unsafe(2), SeqNr.unsafe(3))
          _       <- journal.delete(SeqNr.unsafe(2))
          _       <- journal.append(SeqNr.unsafe(4))
          seqNrs  <- journal.read(SeqRange.unsafe(1, 2))
          _        = seqNrs shouldEqual Nil
          seqNrs  <- journal.read(SeqRange.unsafe(2, 3))
          _        = seqNrs shouldEqual List(SeqNr.unsafe(3))
          seqNrs  <- journal.read(SeqRange.unsafe(3, 4))
          _        = seqNrs shouldEqual List(SeqNr.unsafe(3), SeqNr.unsafe(4))
          seqNrs  <- journal.read(SeqRange.unsafe(4, 5))
          _        = seqNrs shouldEqual List(SeqNr.unsafe(4))
          seqNrs  <- journal.read(SeqRange.unsafe(5, 6))
          _        = seqNrs shouldEqual Nil
          seqNr   <- journal.pointer
          _        = seqNr shouldEqual SeqNr.unsafe(4).some
          _       <- journal.purge
          seqNrs  <- journal.read(SeqRange.all)
          _        = seqNrs shouldEqual Nil
          pointer <- journal.pointer
          _        = pointer shouldEqual none
        } yield Succeeded
      }
    }
  }


  def test(journal: SeqNrJournal[StateT]): Unit = {
    testF[StateT] { f =>
      val (_, result) = f(journal).run(State.empty)
      result
    }
  }


  "Journal" when {

    // TODO add case with failing head cache
    for {
      (headCacheStr, headCache) <- List(
        ("invalid", HeadCache.const(HeadCacheError.invalid.asLeft[HeadInfo].pure[StateT])),
        ("timeout", HeadCache.const(HeadCacheError.timeout(1.second).asLeft[HeadInfo].pure[StateT])),
        ("valid",   StateT.headCache))
    } {

      val name = s"headCache: $headCacheStr"

      s"eventual journal is empty, $name" should {
        val journal = SeqNrJournal(
          EventualJournal.empty[StateT],
          StateT.consumeActionRecords,
          StateT.appendAction,
          headCache)

        test(journal)
      }


      s"kafka journal is empty, $name" should {

        val consumeActionRecords: ConsumeActionRecords[StateT] = {
          (_: Key, _: Partition, from: Offset) => {
            StateT.stream { state =>
              val records = state.records
                .dropWhile(_.offset < from)
                .collect { case action @ ActionRecord(_: Action.Mark, _) => action }
              val state1 = state.copy(recordsToRead = records)
              (state1, StateT.actionRecords)
            }
          }
        }

        val journal = SeqNrJournal(
          StateT.eventualJournal,
          consumeActionRecords,
          StateT.appendAction,
          headCache)

        test(journal)
      }


      s"kafka and eventual journals are consistent, $name" should {
        val journal = SeqNrJournal(
          StateT.eventualJournal,
          StateT.consumeActionRecords,
          StateT.appendAction,
          headCache)

        test(journal)
      }

      for {
        n <- 1 to 3
      } {
        s"kafka and eventual journals are consistent, however eventual offset is $n behind, $name" should {
          val appendAction = new AppendAction[StateT] {

            def apply(action: Action) = {
              StateT { state =>
                val offset = Offset.unsafe(state.records.size)
                val partitionOffset = PartitionOffset(partition = partition, offset = offset)
                val record = ActionRecord(action, partitionOffset)
                val records = state.records.enqueue(record)

                val replicatedState = state.replicatedState(record, Offset.of[Try](offset.value - n) getOrElse Offset.min)
                val state1 = state.copy(records = records, replicatedState = replicatedState)
                (state1, partitionOffset)
              }
            }
          }

          val journal = SeqNrJournal(
            StateT.eventualJournal,
            StateT.consumeActionRecords,
            appendAction,
            headCache)

          test(journal)
        }
      }

      for {
        n <- 1 to 4
      } {
        s"eventual journal is $n actions behind the kafka journal, $name" should {

          val appendAction = new AppendAction[StateT] {

            def apply(action: Action) = {
              StateT { state =>
                val offset = Offset.unsafe(state.records.size)
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
            StateT.eventualJournal,
            StateT.consumeActionRecords,
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

          val appendAction = new AppendAction[StateT] {

            def apply(action: Action) = {
              StateT { state =>
                val offset = Offset.unsafe(state.records.size)
                val partitionOffset = PartitionOffset(partition = partition, offset = offset)
                val record = ActionRecord(action, partitionOffset)
                val records = state.records.enqueue(record)

                val replicatedState = for {
                  actions <- records.dropLast(n)
                  action <- actions.lastOption
                } yield {
                  state.replicatedState(action, Offset.of[Try](offset.value - n) getOrElse Offset.min)
                }
                val state1 = state.copy(
                  records = records,
                  replicatedState = replicatedState getOrElse state.replicatedState)
                (state1, partitionOffset)
              }
            }
          }

          val journal = SeqNrJournal(
            StateT.eventualJournal,
            StateT.consumeActionRecords,
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
  val timestamp: Instant = Instant.now()
  val partition: Partition = Partition.min

  implicit val ec: ExecutionContext = CurrentThreadExecutionContext


  trait SeqNrJournal[F[_]] {

    def append(seqNr: SeqNr, seqNrs: SeqNr*): F[Offset]

    def read(range: SeqRange): F[List[SeqNr]]

    def pointer: F[Option[SeqNr]]

    def delete(to: SeqNr): F[Option[Offset]]

    def purge: F[Option[Offset]]
  }

  object SeqNrJournal {

    def apply[F[_] : Monad](journal: Journal[F]): SeqNrJournal[F] = {

      new SeqNrJournal[F] {

        def append(seqNr: SeqNr, seqNrs: SeqNr*) = {
          val events = for {
            seqNr <- Nel.of(seqNr, seqNrs: _*)
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
          journal
            .read(key, range.from)
            .dropWhile { _.seqNr < range.from }
            .takeWhile { _.seqNr <= range.to }
            .map { _.seqNr }
            .toList
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

        def purge = {
          for {
            partitionOffset <- journal.purge(key)
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
      consumeActionRecords: ConsumeActionRecords[F],
      writeAction: AppendAction[F],
      headCache: HeadCache[F]
    ): SeqNrJournal[F] = {

      implicit val concurrent = ConcurrentOf.fromMonad[F]
      implicit val clock = Clock.const[F](nanos = 0, millis = timestamp.toEpochMilli)
      implicit val parallel = Parallel.identity[F]
      implicit val randomIdOf = RandomIdOf.uuid[F]
      implicit val measureDuration = MeasureDuration.fromClock(clock)
      implicit val fromTry = FromTry.lift[F]
      implicit val fail = Fail.lift[F]
      implicit val fromAttempt = FromAttempt.lift[F]
      implicit val fromJsResult = FromJsResult.lift[F]
      val log = Log.empty[F]

      val journal = Journal[F](
        origin = None,
        eventual = eventual,
        consumeActionRecords = consumeActionRecords,
        appendAction = writeAction,
        headCache = headCache,
        payloadToEvents = PayloadToEvents[F],
        eventsToPayload = EventsToPayload[F],
        log = log)
        .withLog(log)
        .withMetrics(Journal.Metrics.empty[F])
      SeqNrJournal(journal)
    }
  }


  final case class State(
    records: Queue[ActionRecord[Action]] = Queue.empty,
    replicatedState: EventualJournalOf.State = EventualJournalOf.State.empty,
    recordsToRead: Queue[ActionRecord[Action]] = Queue.empty)

  object State {
    val empty: State = State()
  }


  type StateT[A] = cats.data.StateT[Id, State, A]

  object StateT {

    val eventualJournal: EventualJournal[StateT] = new EventualJournal[StateT] {

      def pointers(topic: Topic) = {
        StateT { state =>
          val topicPointers = state.replicatedState.offset.fold(TopicPointers.empty) { offset =>
            val pointers = Map((partition, offset))
            TopicPointers(pointers)
          }

          (state, topicPointers)
        }
      }

      def read(key: Key, from: SeqNr) = {
        val events = StateT { state =>
          val events = state.replicatedState.events.toList.filter(_.seqNr >= from)
          (state, events)
        }

        for {
          events <- Stream.lift(events)
          event <- Stream[StateT].apply(events)
        } yield {
          event
        }
      }

      def pointer(key: Key) = {
        StateT { state =>

          val seqNr = state.replicatedState.events.lastOption.map(_.event.seqNr)
          val pointer = for {
            seqNr <- seqNr max state.replicatedState.deleteTo
            offset <- state.replicatedState.offset
          } yield {
            val partitionOffset = PartitionOffset(partition, offset)
            JournalPointer(partitionOffset, seqNr)
          }

          (state, pointer)
        }
      }
    }


    val actionRecords: Stream[StateT, ActionRecord[Action]] = {
      val result = StateT { state =>
        state.recordsToRead.dequeueOption match {
          case Some((record, records)) => (state.copy(recordsToRead = records), Stream[StateT].single(record))
          case None                    => (state, Stream[StateT].empty[ActionRecord[Action]])
        }
      }
      Stream.repeat(result).flatten
    }


    val consumeActionRecords: ConsumeActionRecords[StateT] = {
      (_: Key, _: Partition, from: Offset) => {
        StateT.stream { state =>
          val records = state.records.dropWhile(_.offset < from)
          val state1 = state.copy(recordsToRead = records)
          (state1, actionRecords)
        }
      }
    }


    val appendAction: AppendAction[StateT] = {
      action: Action => {
        StateT { state =>
          val offset = Offset.unsafe(state.records.size)
          val partitionOffset = PartitionOffset(partition = partition, offset = offset)
          val record = ActionRecord(action, partitionOffset)
          val records = state.records.enqueue(record)

          val replicatedState = state.replicatedState(record)
          val state1 = state.copy(records = records, replicatedState = replicatedState)
          (state1, partitionOffset)
        }
      }
    }


    val headCache: HeadCache[StateT] = {
      (_: Key, _: Partition, _: Offset) => {

        StateT { state =>
          val headInfo = state
            .records
            .foldLeft(HeadInfo.empty) { (info, record) => info(record.action.header, record.offset) }
          (state, headInfo.asRight)
        }
      }
    }

    def apply[A](f: State => (State, A)): StateT[A] = cats.data.StateT[Id, State, A](f)

    def stream[A](f: State => (State, Stream[StateT, A])): Stream[StateT, A] = Stream.lift(apply(f)).flatten
  }


  // TODO implement via mocking EventualCassandra
  object EventualJournalOf {

    final case class State(
      events: Queue[EventRecord] = Queue.empty,
      deleteTo: Option[SeqNr] = None,
      offset: Option[Offset] = None) {

      def apply(record: ActionRecord[Action]): State = {
        apply(record, record.offset)
      }

      def apply(record: ActionRecord[Action], offset: Offset): State = {

        implicit val fromAttempt = FromAttempt.lift[Try]
        implicit val fromJsResult = FromJsResult.lift[Try]
        implicit val payloadToEvents = PayloadToEvents[Try]

        def updateOffset = copy(offset = offset.some)

        def onAppend(action: Action.Append) = {
          val payloadAndType = PayloadAndType(action)
          val batch = for {
            event <- payloadToEvents(payloadAndType).get.events
          } yield {
            val partitionOffset = PartitionOffset(partition, record.offset)
            EventRecord(action, event, partitionOffset)
          }
          copy(events = events.enqueue(batch.toList), offset = offset.some)
        }

        def onDelete(action: Action.Delete) = {
          events.lastOption.fold(updateOffset) { last =>
            val lastSeqNr = last.event.seqNr
            if (lastSeqNr <= action.to) {
              copy(
                events = Queue.empty,
                deleteTo = lastSeqNr.some,
                offset = offset.some)
            } else {
              val left = events.dropWhile { _.event.seqNr <= action.to }
              copy(
                events = left,
                deleteTo = action.to.some,
                offset = offset.some)
            }
          }
        }

        def onPurge = {
          events
            .lastOption
            .fold(updateOffset) { _ =>
              copy(
                events = Queue.empty,
                deleteTo = None,
                offset = offset.some)
            }
        }

        record.action match {
          case a: Action.Append => onAppend(a)
          case _: Action.Mark   => updateOffset
          case a: Action.Delete => onDelete(a)
          case _: Action.Purge  => onPurge
        }
      }
    }

    object State {
      val empty: State = State()
    }
  }


  implicit class TestFutureOps[T](val self: Future[T]) extends AnyVal {

    def get(): T = self.value.get.get
  }

  implicit class QueueOps[T](val self: Queue[T]) extends AnyVal {
    
    def dropLast(n: Int): Option[Queue[T]] = {
      if (self.size <= n) none
      else self.dropRight(n).some
    }
  }
}