package com.evolutiongaming.kafka.journal

import cats.Monad
import cats.data.NonEmptyList as Nel
import cats.effect.kernel.Sync
import cats.effect.unsafe.implicits.global
import cats.effect.{Clock, IO}
import cats.syntax.all.*
import com.evolutiongaming.catshelper.ClockHelper.*
import com.evolutiongaming.catshelper.{FromTry, Log, MeasureDuration, RandomIdOf}
import com.evolutiongaming.concurrent.CurrentThreadExecutionContext
import com.evolutiongaming.kafka.journal.TestJsonCodec.instance
import com.evolutiongaming.kafka.journal.conversions.{KafkaRead, KafkaWrite}
import com.evolutiongaming.kafka.journal.eventual.*
import com.evolutiongaming.kafka.journal.util.Fail
import com.evolutiongaming.kafka.journal.util.SkafkaHelper.*
import com.evolutiongaming.kafka.journal.util.StreamHelper.*
import com.evolutiongaming.skafka.{Offset, Partition, Topic}
import com.evolutiongaming.sstream.Stream
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.{Assertion, Succeeded}

import java.time.Instant
import scala.collection.immutable.Queue
import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try

class JournalSpec extends AnyWordSpec with Matchers {
  import JournalSpec.*

  // TODO add test when Kafka missing it's tail comparing to eventual
  def testF[F[_]: Monad](withJournal: (SeqNrJournal[F] => F[Assertion]) => Assertion): Unit = {
    for {
      size <- 0 to 5
      seqNrs = (1 to size).toList.map { a => SeqNr.unsafe(a) }
      combination <- Combinations(seqNrs)
    } {

      val seqNrLast = seqNrs.lastOption

      def createAndAppend(f: (SeqNrJournal[F], Option[Offset]) => F[Assertion]) = {
        withJournal { journal =>
          def append(seqNrs: Nel[SeqNr]) = {
            journal
              .append(seqNrs.head, seqNrs.tail*)
              .map { _.some }
          }
          for {
            offset <- combination.foldLeftM(none[Offset]) { (_, seqNrs) => append(seqNrs) }
            offsetNext = offset.map { _.inc[Try].get }
            result <- f(journal, offsetNext)
          } yield result
        }
      }

      val name = combination
        .map { _.toList.mkString("[", ",", "]") }
        .mkString(",")

      s"append, $name" in {
        createAndAppend {
          case (journal, _) =>
            for {
              a <- journal.read(SeqRange.all)
            } yield {
              a shouldEqual seqNrs
            }
        }
      }

      s"read, $name" in {
        createAndAppend {
          case (journal, _) =>
            for {
              a <- journal.read(SeqRange.all)
              _ = a shouldEqual seqNrs
              last = seqNrLast getOrElse SeqNr.min
              a <- journal.read(SeqNr.min to last)
              _ = a shouldEqual seqNrs
              a <- journal.read(SeqNr.min to last.next[Option].getOrElse(last))
            } yield {
              a shouldEqual seqNrs
            }
        }
      }

      s"delete all, $name" in {
        createAndAppend {
          case (journal, _) =>
            for {
              _ <- seqNrLast.fold(().pure[F]) { seqNr => journal.delete(seqNr.toDeleteTo).void }
              a <- journal.read(SeqRange.all)
              _ = a shouldEqual Nil
              a <- journal.pointer
            } yield {
              a shouldEqual seqNrLast
            }
        }
      }

      s"delete SeqNr.Max, $name" in {
        createAndAppend {
          case (journal, _) =>
            for {
              _ <- journal.delete(DeleteTo.max)
              a <- journal.read(SeqRange.all)
              _ = a shouldEqual Nil
              a <- journal.pointer
            } yield {
              a shouldEqual seqNrLast
            }
        }
      }

      s"delete SeqNr.Min, $name" in {
        createAndAppend {
          case (journal, offset) =>
            for {
              a <- journal.delete(DeleteTo.min)
              _ = a shouldEqual offset.map { _.inc[Try].get }
              a <- journal.read(SeqRange.all)
              _ = a shouldEqual seqNrs.dropWhile(_ <= SeqNr.min)
              a <- journal.pointer
              _ = a shouldEqual seqNrLast
            } yield Succeeded
        }
      }

      s"purge, $name" in {
        createAndAppend {
          case (journal, _) =>
            for {
              _ <- journal.purge
              a <- journal.read(SeqRange.all)
              _ = a shouldEqual List.empty
              a <- journal.pointer
              _ = a shouldEqual none
            } yield Succeeded
        }
      }

      s"lastSeqNr, $name" in {
        createAndAppend {
          case (journal, _) =>
            for {
              a <- journal.pointer
              _ = a shouldEqual seqNrLast
            } yield Succeeded
        }
      }

      for {
        _ <- seqNrLast
        seqNr <- seqNrs.tail.lastOption
      } {

        s"delete except last, $name" in {
          createAndAppend {
            case (journal, _) =>
              for {
                _ <- journal.delete(seqNr.toDeleteTo)
                seqNrs <- journal.read(SeqRange.all)
                _ = seqNrs shouldEqual seqNrs.dropWhile(_ <= seqNr)
                seqNr <- journal.pointer
              } yield {
                seqNr shouldEqual seqNrLast
              }
          }
        }

        s"read tail, $name" in {
          createAndAppend {
            case (journal, _) =>
              for {
                seqNrs <- journal.read(seqNr to SeqNr.max)
              } yield {
                seqNrs shouldEqual seqNrs.dropWhile(_ < seqNr)
              }
          }
        }
      }
    }

    s"read SeqNr.Max" in {
      withJournal { journal =>
        for {
          seqNrs <- journal.read(SeqRange(SeqNr.max))
          _ = seqNrs shouldEqual Nil
          _ <- journal.append(SeqNr.unsafe(1))
          seqNrs <- journal.read(SeqRange(SeqNr.max))
        } yield {
          seqNrs shouldEqual Nil
        }
      }
    }

    s"append, delete, append, delete, append, read, lastSeqNr, purge" in {
      withJournal { journal =>
        for {
          _ <- journal.append(SeqNr.unsafe(1))
          _ <- journal.delete(SeqNr.unsafe(3).toDeleteTo)
          _ <- journal.append(SeqNr.unsafe(2), SeqNr.unsafe(3))
          _ <- journal.delete(SeqNr.unsafe(2).toDeleteTo)
          _ <- journal.append(SeqNr.unsafe(4))
          seqNrs <- journal.read(SeqRange.unsafe(1, 2))
          _ = seqNrs shouldEqual Nil
          seqNrs <- journal.read(SeqRange.unsafe(2, 3))
          _ = seqNrs shouldEqual List(SeqNr.unsafe(3))
          seqNrs <- journal.read(SeqRange.unsafe(3, 4))
          _ = seqNrs shouldEqual List(SeqNr.unsafe(3), SeqNr.unsafe(4))
          seqNrs <- journal.read(SeqRange.unsafe(4, 5))
          _ = seqNrs shouldEqual List(SeqNr.unsafe(4))
          seqNrs <- journal.read(SeqRange.unsafe(5, 6))
          _ = seqNrs shouldEqual Nil
          seqNr <- journal.pointer
          _ = seqNr shouldEqual SeqNr.unsafe(4).some
          _ <- journal.purge
          seqNrs <- journal.read(SeqRange.all)
          _ = seqNrs shouldEqual Nil
          pointer <- journal.pointer
          _ = pointer shouldEqual none
        } yield Succeeded
      }
    }

    s"read record completely" in {
      withJournal { journal =>
        for {
          _ <- journal.append(SeqNr.unsafe(1), SeqNr.unsafe(2))
          a <- journal.read(SeqRange.unsafe(1, 2))
        } yield {
          a shouldEqual List(SeqNr.unsafe(1), SeqNr.unsafe(2))
        }
      }
    }

    s"read record partially" in {
      withJournal { journal =>
        for {
          _ <- journal.append(SeqNr.unsafe(1), SeqNr.unsafe(2))
          a <- journal.read(SeqRange.unsafe(2))
        } yield {
          a shouldEqual List(SeqNr.unsafe(2))
        }
      }
    }
  }

  "Journal" when {

    for {
      (headCacheStr, headCache) <-
        List(("invalid", HeadCache.const(none[HeadInfo].pure[StateT])), ("valid", StateT.headCache))
      (duplicatesStr, consumeActionRecordsOf, eventualJournalOf) <- List(
        ("off", (a: ConsumeActionRecords[StateT]) => a, (a: EventualJournal[StateT]) => a),
        ("on", (a: ConsumeActionRecords[StateT]) => a.withDuplicates, (a: EventualJournal[StateT]) => a),
      )
    } {

      def test(
        eventual: EventualJournal[StateT],
        consumeActionRecords: ConsumeActionRecords[StateT],
        produceAction: ProduceAction[StateT],
        headCache: HeadCache[StateT],
      ): Unit = {
        val journal =
          SeqNrJournal(
            eventualJournalOf(eventual),
            consumeActionRecordsOf(consumeActionRecords),
            produceAction,
            headCache,
          )

        testF[StateT] { f =>
          f(journal)
            .run(State.empty)
            .map { case (_, result) => result }
            .unsafeRunSync()
        }
      }

      val name = s"headCache: $headCacheStr, duplicates: $duplicatesStr"

      s"eventual journal is empty, $name" should {
        test(EventualJournal.empty[StateT], StateT.consumeActionRecords, StateT.produceAction, headCache)
      }

      s"kafka journal is empty, $name" should {

        val consumeActionRecords: ConsumeActionRecords[StateT] = { (_: Key, _: Partition, from: Offset) =>
          {
            StateT.stream { state =>
              val records = state
                .records
                .dropWhile { _.offset < from }
                .collect { case action @ ActionRecord(_: Action.Mark, _) => action }
              val state1 = state.copy(recordsToRead = records)
              (state1, StateT.actionRecords).pure[IO]
            }
          }
        }

        test(StateT.eventualJournal, consumeActionRecords, StateT.produceAction, headCache)
      }

      s"kafka and eventual journals are consistent, $name" should {
        test(StateT.eventualJournal, StateT.consumeActionRecords, StateT.produceAction, headCache)
      }

      for {
        n <- 1 to 3
      } {
        s"kafka and eventual journals are consistent, however eventual offset is $n behind, $name" should {
          val produceAction = new ProduceAction[StateT] {

            def apply(action: Action) = {
              StateT { state =>
                val offset = Offset.unsafe(state.records.size)
                val partitionOffset = PartitionOffset(partition = partition, offset = offset)
                val record = ActionRecord(action, partitionOffset)
                val records = state.records.enqueue(record)

                val replicatedState =
                  state.replicatedState(record, Offset.of[Try](offset.value - n) getOrElse Offset.min)
                val state1 = state.copy(records = records, replicatedState = replicatedState)
                (state1, partitionOffset)
              }
            }
          }

          test(StateT.eventualJournal, StateT.consumeActionRecords, produceAction, headCache)
        }
      }

      for {
        n <- 1 to 4
      } {
        s"eventual journal is $n actions behind the kafka journal, $name" should {

          val produceAction = new ProduceAction[StateT] {

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
                val state1 =
                  state.copy(records = records, replicatedState = replicatedState getOrElse state.replicatedState)
                (state1, partitionOffset)
              }
            }
          }

          test(StateT.eventualJournal, StateT.consumeActionRecords, produceAction, headCache)
        }
      }

      for {
        n <- 1 to 3
        nn = n + 1
      } {
        s"eventual journal is $n actions behind and pointer is $nn behind the kafka journal, $name" should {

          val produceAction = new ProduceAction[StateT] {

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
                val state1 =
                  state.copy(records = records, replicatedState = replicatedState getOrElse state.replicatedState)
                (state1, partitionOffset)
              }
            }
          }

          test(StateT.eventualJournal, StateT.consumeActionRecords, produceAction, headCache)
        }
      }
    }
  }
}

object JournalSpec {
  val key: Key = Key(topic = "topic", id = "id")
  val timestamp: Instant = Instant.now()
  val partition: Partition = Partition.min

  implicit val ec: ExecutionContext = CurrentThreadExecutionContext

  trait SeqNrJournal[F[_]] {

    def append(seqNr: SeqNr, seqNrs: SeqNr*): F[Offset]

    def read(range: SeqRange): F[List[SeqNr]]

    def pointer: F[Option[SeqNr]]

    def delete(to: DeleteTo): F[Option[Offset]]

    def purge: F[Option[Offset]]
  }

  object SeqNrJournal {

    def apply[F[_]: Monad, A](
      journals: Journals[F],
    )(implicit
      kafkaRead: KafkaRead[F, A],
      eventualRead: EventualRead[F, A],
      kafkaWrite: KafkaWrite[F, A],
    ): SeqNrJournal[F] = {
      apply(journals(key))
    }

    def apply[F[_]: Monad, A](
      journal: Journal[F],
    )(implicit
      kafkaRead: KafkaRead[F, A],
      eventualRead: EventualRead[F, A],
      kafkaWrite: KafkaWrite[F, A],
    ): SeqNrJournal[F] = {

      new SeqNrJournal[F] {

        def append(seqNr: SeqNr, seqNrs: SeqNr*) = {
          val events = for {
            seqNr <- Nel.of(seqNr, seqNrs*)
          } yield {
            Event[A](seqNr)
          }
          for {
            partitionOffset <- journal.append(events)
          } yield {
            partitionOffset.offset
          }
        }

        def read(range: SeqRange) = {
          journal
            .read(range.from)
            .dropWhile { _.seqNr < range.from }
            .takeWhile { _.seqNr <= range.to }
            .map { _.seqNr }
            .toList
        }

        def pointer = journal.pointer

        def delete(to: DeleteTo) = {
          for {
            partitionOffset <- journal.delete(to)
          } yield for {
            partitionOffset <- partitionOffset
          } yield {
            partitionOffset.offset
          }
        }

        def purge = {
          for {
            partitionOffset <- journal.purge
          } yield for {
            partitionOffset <- partitionOffset
          } yield {
            partitionOffset.offset
          }
        }
      }
    }

    def apply[F[_]: Sync](
      eventual: EventualJournal[F],
      consumeActionRecords: ConsumeActionRecords[F],
      produceAction: ProduceAction[F],
      headCache: HeadCache[F],
    ): SeqNrJournal[F] = {
      implicit val clock: Clock[F] = Clock.const[F](nanos = 0, millis = timestamp.toEpochMilli)
      implicit val randomIdOf: RandomIdOf[F] = RandomIdOf.uuid[F]
      implicit val measureDuration: MeasureDuration[F] = MeasureDuration.fromClock(clock)
      implicit val fromTry: FromTry[F] = FromTry.lift[F]
      implicit val fail: Fail[F] = Fail.lift[F]
      implicit val fromAttempt: FromAttempt[F] = FromAttempt.lift[F]
      implicit val fromJsResult: FromJsResult[F] = FromJsResult.lift[F]
      val log = Log.empty[F]

      val journal = Journals[F](
        eventual = eventual,
        consumeActionRecords = consumeActionRecords,
        produce = Produce(produceAction, none),
        headCache = headCache,
        log = log,
        conversionMetrics = none,
      )
        .withLog(log)
        .withMetrics(JournalMetrics.empty[F])
      SeqNrJournal(journal)
    }
  }

  final case class State(
    records: Queue[ActionRecord[Action]] = Queue.empty,
    replicatedState: EventualJournalOf.State = EventualJournalOf.State.empty,
    recordsToRead: Queue[ActionRecord[Action]] = Queue.empty,
  )

  object State {
    val empty: State = State()
  }

  type StateT[A] = cats.data.StateT[IO, State, A]

  object StateT {

    val eventualJournal: EventualJournal[StateT] = new EventualJournal[StateT] {

      def offset(topic: Topic, partition: Partition): StateT[Option[Offset]] = {
        StateT { state =>
          (state, state.replicatedState.offset)
        }
      }

      def read(key: Key, from: SeqNr) = {
        val events = StateT { state =>
          val events = state.replicatedState.events.toList.filter(_.seqNr >= from)
          (state, events)
        }

        for {
          events <- events.toStream
          event <- Stream[StateT].apply(events)
        } yield {
          event
        }
      }

      def pointer(key: Key) = {
        StateT { state =>
          val seqNr = state.replicatedState.events.lastOption.map(_.event.seqNr)
          val pointer = for {
            seqNr <- seqNr max state.replicatedState.deleteTo.map { _.value }
            offset <- state.replicatedState.offset
          } yield {
            val partitionOffset = PartitionOffset(partition, offset)
            JournalPointer(partitionOffset, seqNr)
          }

          (state, pointer)
        }
      }

      def ids(topic: Topic) = {
        Stream[StateT].single(key.id)
      }
    }

    val actionRecords: Stream[StateT, ActionRecord[Action]] = {
      val result = StateT { state =>
        state.recordsToRead.dequeueOption match {
          case Some((record, records)) => (state.copy(recordsToRead = records), Stream[StateT].single(record))
          case None => (state, Stream[StateT].empty[ActionRecord[Action]])
        }
      }
      Stream
        .repeat(result)
        .flatten
    }

    val consumeActionRecords: ConsumeActionRecords[StateT] = { (_: Key, _: Partition, from: Offset) =>
      {
        StateT.stream { state =>
          val records = state
            .records
            .dropWhile { _.offset < from }
          val state1 = state.copy(recordsToRead = records)
          (state1, actionRecords).pure[IO]
        }
      }
    }

    val produceAction: ProduceAction[StateT] = { (action: Action) =>
      {
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

    val headCache: HeadCache[StateT] = { (_: Key, _: Partition, _: Offset) =>
      {

        StateT { state =>
          val headInfo = state
            .records
            .foldLeft(HeadInfo.empty) { (info, record) => info(record.action.header, record.offset) }
          (state, headInfo.some)
        }
      }
    }

    def apply[A](f: State => (State, A)): StateT[A] = cats.data.StateT[IO, State, A](s => IO.delay(f(s)))

    def of[A](f: State => IO[(State, A)]): StateT[A] = cats.data.StateT[IO, State, A](s => f(s))

    def stream[A](f: State => IO[(State, Stream[StateT, A])]): Stream[StateT, A] = of(f).toStream.flatten
  }

  // TODO implement via mocking EventualCassandra
  object EventualJournalOf {

    final case class State(
      events: Queue[EventRecord[EventualPayloadAndType]] = Queue.empty,
      deleteTo: Option[DeleteTo] = None,
      offset: Option[Offset] = None,
    ) {

      def apply(record: ActionRecord[Action]): State = {
        apply(record, record.offset)
      }

      def apply(record: ActionRecord[Action], offset: Offset): State = {

        implicit val fromAttempt: FromAttempt[Try] = FromAttempt.lift[Try]
        implicit val fromJsResult: FromJsResult[Try] = FromJsResult.lift[Try]

        val kafkaRead = KafkaRead.summon[Try, Payload]
        val eventualWrite = EventualWrite.summon[Try, Payload]

        def updateOffset = copy(offset = offset.some)

        def onAppend(action: Action.Append) = {
          val payloadAndType = action.toPayloadAndType
          val events1 = kafkaRead(payloadAndType).get
          val batch = for {
            event <- events1.events
          } yield {
            val partitionOffset = PartitionOffset(partition, record.offset)
            EventRecord(action, event.map(eventualWrite(_).get), partitionOffset, events1.metadata)
          }
          copy(events = events.enqueueAll(batch.toList), offset = offset.some)
        }

        def onDelete(action: Action.Delete) = {
          events.lastOption.fold(updateOffset) { last =>
            val lastSeqNr = last.event.seqNr
            if (lastSeqNr <= action.to.value) {
              copy(events = Queue.empty, deleteTo = lastSeqNr.toDeleteTo.some, offset = offset.some)
            } else {
              val left = events.dropWhile { _.event.seqNr <= action.to.value }
              copy(events = left, deleteTo = action.to.some, offset = offset.some)
            }
          }
        }

        def onPurge = {
          events
            .lastOption
            .fold(updateOffset) { _ =>
              copy(events = Queue.empty, deleteTo = None, offset = offset.some)
            }
        }

        record.action match {
          case a: Action.Append => onAppend(a)
          case _: Action.Mark => updateOffset
          case a: Action.Delete => onDelete(a)
          case _: Action.Purge => onPurge
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

  implicit class ConsumeActionRecordsOps[F[_]](val self: ConsumeActionRecords[F]) extends AnyVal {
    def withDuplicates(
      implicit
      F: Monad[F],
    ): ConsumeActionRecords[F] = new ConsumeActionRecords[F] {
      def apply(key: Key, partition: Partition, from: Offset) = {
        self
          .apply(key, partition, from)
          .withDuplicates
      }
    }
  }

  implicit class StreamOps[F[_], A](val self: Stream[F, A]) extends AnyVal {
    def withDuplicates(
      implicit
      F: Monad[F],
    ): Stream[F, A] = {
      self.flatMap { a =>
        List
          .fill(2)(a)
          .toStream1[F]
      }
    }
  }
}
