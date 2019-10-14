package com.evolutiongaming.kafka.journal

import java.time.Instant

import cats.data.IndexedStateT
import cats.effect.{ExitCase, Resource}
import cats.implicits._
import com.evolutiongaming.catshelper.BracketThrowable
import com.evolutiongaming.kafka.journal.util.BracketFromMonadError
import com.evolutiongaming.skafka.{Offset, Partition}
import org.scalatest.{FunSuite, Matchers}
import scodec.bits.ByteVector

import scala.util.{Success, Try}

class StreamActionRecordsSpec extends FunSuite with Matchers {
  import StreamActionRecordsSpec._

  test("no offsets") {
    val records = List(
      Pointer(seqNr = 1L, offset = 11L),
      Pointer(seqNr = 2L, offset = 12L),
      Pointer(seqNr = 3L, offset = 13L))
    val result = seqNrs(None, None, records)
    result shouldEqual List((1L, 11L), (2L, 12L), (3L, 13L))
  }

  test("replicated offset") {
    val records = List(
      Pointer(seqNr = 1L, offset = 11L),
      Pointer(seqNr = 2L, offset = 12L),
      Pointer(seqNr = 3L, offset = 13L))
    val result = seqNrs(Some(11L), None, records)
    result shouldEqual List((2L, 12L), (3L, 13L))
  }

  test("replicated and queried offsets") {
    val records = List(
      Pointer(seqNr = 1L, offset = 11L),
      Pointer(seqNr = 2L, offset = 12L),
      Pointer(seqNr = 3L, offset = 13L))
    val result = seqNrs(Some(11L), Some(12L), records)
    result shouldEqual List((3L, 13L))
  }

  test("queried offsets covers all kafka records") {
    val records = List(
      Pointer(seqNr = 1L, offset = 1L),
      Pointer(seqNr = 2L, offset = 2L),
      Pointer(seqNr = 3L, offset = 3L))
    val result = seqNrs(Some(11L), Some(13L), records)
    result shouldEqual Nil
  }
}

object StreamActionRecordsSpec {

  implicit val bracket: BracketThrowable[StateT] = new BracketFromMonadError[StateT, Throwable] {

    val F = IndexedStateT.catsDataMonadErrorForIndexedStateT(catsStdInstancesForTry)

    def bracketCase[A, B](
      acquire: StateT[A])(
      use: A => StateT[B])(
      release: (A, ExitCase[Throwable]) => StateT[Unit]
    ) = {

      def onError(a: A)(e: Throwable) = for {
        _ <- release(a, ExitCase.error(e))
        b <- raiseError[B](e)
      } yield b

      for {
        a <- acquire
        b <- handleErrorWith(use(a))(onError(a))
        _ <- release(a, ExitCase.complete)
      } yield b
    }
  }


  private def seqNrs(
    replicated: Option[Offset],
    offset: Option[Offset],
    pointers: List[Pointer]
  ) = {

    val timestamp = Instant.now()
    val key = Key(topic = "topic", id = "id")

    val (marker, markRecord) = {
      val offset = pointers.lastOption.fold(1L) { _.offset + 1 }
      val mark = Action.Mark(key, timestamp, ActionHeader.Mark("mark", None))
      val partitionOffset = PartitionOffset(offset = offset)
      val record = ActionRecord(mark, partitionOffset)
      val marker = Marker(mark.id, partitionOffset)
      (marker, record)
    }

    val appendRecords = for {
      pointer <- pointers
    } yield {
      val range = SeqRange.unsafe(pointer.seqNr)
      val metadata = Metadata.empty
      val header = ActionHeader.Append(
        range = range,
        origin = none,
        payloadType = PayloadType.Json,
        metadata = metadata,
        expireAfter = none)
      val action = Action.Append(key, timestamp, header, ByteVector.empty, Headers.empty)
      ActionRecord(action, PartitionOffset(offset = pointer.offset))
    }
    val records = appendRecords :+ markRecord

    val readActionsOf = new ReadActionsOf[StateT] {

      def apply(key: Key, partition: Partition, from: Offset) = {

        val readActions: ReadActions.Type[StateT] = StateT { s =>
          val records = s.records.dropWhile(_.offset < from)
          records match {
            case h :: t => (s.copy(records = t), List(h))
            case _      => (s, Nil)
          }
        }
        Resource.pure[StateT, ReadActions.Type[StateT]](readActions)
      }
    }

    def seqNrAndOffset(action: Action.Append, partitionOffset: PartitionOffset) = {
      for {
        seqNr <- action.range.toNel.toList
      } yield {
        (seqNr.value, partitionOffset.offset)
      }
    }

    val actionRecords = StreamActionRecords[StateT](key, SeqNr.min, marker, replicated, readActionsOf)
    val (_, result) = actionRecords(offset)
      .collect { case ActionRecord(a: Action.Append, partitionOffset) => seqNrAndOffset(a, partitionOffset) }
      .toList
      .run(State(records))
      .get
    result.flatten
  }


  final case class Pointer(seqNr: Long, offset: Offset)


  final case class State(records: List[ActionRecord[Action]])


  type StateT[A] = cats.data.StateT[Try, State, A]

  object StateT {

    def apply[A](f: State => (State, A)): StateT[A] = {
      cats.data.StateT[Try, State, A] { state => Success(f(state)) }
    }
  }
}