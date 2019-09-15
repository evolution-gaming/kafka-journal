package com.evolutiongaming.kafka.journal

import java.time.Instant

import cats.effect.{ExitCase, Resource, Sync}
import cats.implicits._
import com.evolutiongaming.skafka.{Offset, Partition}
import org.scalatest.{FunSuite, Matchers}
import scodec.bits.ByteVector

import scala.util.{Failure, Success, Try}

class FoldActionsSpec extends FunSuite with Matchers {

  import FoldActionsSpec._

  private implicit val sync = SyncOf[StateT]

  test("no offsets") {
    val records = List(
      Pointer(seqNr = 1l, offset = 11l),
      Pointer(seqNr = 2l, offset = 12l),
      Pointer(seqNr = 3l, offset = 13l))
    val result = seqNrs(None, None, records)
    result shouldEqual List((1l, 11l), (2l, 12l), (3l, 13l))
  }

  test("replicated offset") {
    val records = List(
      Pointer(seqNr = 1l, offset = 11l),
      Pointer(seqNr = 2l, offset = 12l),
      Pointer(seqNr = 3l, offset = 13l))
    val result = seqNrs(Some(11l), None, records)
    result shouldEqual List((2l, 12l), (3l, 13l))
  }

  test("replicated and queried offsets") {
    val records = List(
      Pointer(seqNr = 1l, offset = 11l),
      Pointer(seqNr = 2l, offset = 12l),
      Pointer(seqNr = 3l, offset = 13l))
    val result = seqNrs(Some(11l), Some(12l), records)
    result shouldEqual List((3l, 13l))
  }

  test("queried offsets covers all kafka records") {
    val records = List(
      Pointer(seqNr = 1l, offset = 1l),
      Pointer(seqNr = 2l, offset = 2l),
      Pointer(seqNr = 3l, offset = 3l))
    val result = seqNrs(Some(11l), Some(13l), records)
    result shouldEqual Nil
  }
}

object FoldActionsSpec {

  implicit val bracketCase: BracketCase[StateT] = new BracketCase[StateT] {

    def bracketCase[A, B](acquire: StateT[A])(use: A => StateT[B])(release: (A, ExitCase[Throwable]) => StateT[Unit]) = {

      def exitCase(b: Try[(State, B)]) = b match {
        case Success(_) => ExitCase.complete[Throwable]
        case Failure(e) => ExitCase.error(e)
      }

      cats.data.StateT { s =>
        for {
          sa     <- acquire.run(s)
          (s, a)  = sa
          sb      = use(a).run(s)
          ec      = exitCase(sb)
          _      <- release(a, ec).run(s)
          b      <- sb
        } yield b
      }
    }
  }

  
  implicit val suspend: Suspend[StateT] = new Suspend[StateT] {
    def suspend[A](thunk: => StateT[A]): StateT[A] = thunk
  }


  private def seqNrs(
    replicated: Option[Offset],
    offset: Option[Offset],
    pointers: List[Pointer]
  )(implicit F: Sync[StateT]) = {

    val timestamp = Instant.now()
    val key = Key(topic = "topic", id = "id")

    val (marker, markRecord) = {
      val offset = pointers.lastOption.fold(1l) { _.offset + 1 }
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
      val header = ActionHeader.Append(range, None, PayloadType.Json, metadata)
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

    val foldActions = FoldActions[StateT](key, SeqNr.min, marker, replicated, readActionsOf)
    val (_, result) = foldActions(offset)
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