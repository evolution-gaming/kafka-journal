package com.evolution.kafka.journal

import cats.data.IndexedStateT
import cats.effect.kernel.{CancelScope, Poll}
import cats.implicits.*
import cats.syntax.all.none
import com.evolutiongaming.catshelper.BracketThrowable
import com.evolution.kafka.journal.util.MonadCancelFromMonadError
import com.evolutiongaming.skafka.{Offset, Partition}
import com.evolutiongaming.sstream.Stream
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import scodec.bits.ByteVector

import java.time.Instant
import scala.util.{Success, Try}

class StreamActionRecordsSpec extends AnyFunSuite with Matchers {
  import StreamActionRecordsSpec.*

  test("no offsets") {
    val records =
      List(Pointer(seqNr = 1L, offset = 11L), Pointer(seqNr = 2L, offset = 12L), Pointer(seqNr = 3L, offset = 13L))
    val result = seqNrs(None, None, records)
    result shouldEqual List((1L, 11L), (2L, 12L), (3L, 13L))
  }

  test("replicated offset") {
    val records =
      List(Pointer(seqNr = 1L, offset = 11L), Pointer(seqNr = 2L, offset = 12L), Pointer(seqNr = 3L, offset = 13L))
    val result = seqNrs(11L.some, None, records)
    result shouldEqual List((2L, 12L), (3L, 13L))
  }

  test("replicated and queried offsets") {
    val records =
      List(Pointer(seqNr = 1L, offset = 11L), Pointer(seqNr = 2L, offset = 12L), Pointer(seqNr = 3L, offset = 13L))
    val result = seqNrs(11L.some, 12L.some, records)
    result shouldEqual List((3L, 13L))
  }

  test("queried offsets covers all kafka records") {
    val records =
      List(Pointer(seqNr = 1L, offset = 1L), Pointer(seqNr = 2L, offset = 2L), Pointer(seqNr = 3L, offset = 3L))
    val result = seqNrs(11L.some, 13L.some, records)
    result shouldEqual Nil
  }
}

object StreamActionRecordsSpec {

  implicit val bracket: BracketThrowable[StateT] = new MonadCancelFromMonadError[StateT, Throwable] {

    val F = IndexedStateT.catsDataMonadErrorForIndexedStateT(catsStdInstancesForTry)

    override def rootCancelScope: CancelScope = CancelScope.Uncancelable

    override def forceR[A, B](fa: StateT[A])(fb: StateT[B]): StateT[B] = F.redeemWith(fa)(_ => fb, _ => fb)

    override def uncancelable[A](body: Poll[StateT] => StateT[A]): StateT[A] = body(new Poll[StateT] {
      override def apply[X](fa: StateT[X]): StateT[X] = fa
    })

    override def canceled: StateT[Unit] = F.unit

    override def onCancel[A](fa: StateT[A], fin: StateT[Unit]): StateT[A] = fa
  }

  private def seqNrs(
    replicated: Option[Long],
    offset: Option[Long],
    pointers: List[Pointer],
  ) = {

    val timestamp = Instant.now()
    val key = Key(topic = "topic", id = "id")

    val (marker, markRecord) = {
      val offset = pointers.lastOption.fold(1L) { _.offset + 1 }
      val mark = Action.Mark(key, timestamp, ActionHeader.Mark("mark", none, Version.current.some))
      val partitionOffset = PartitionOffset(offset = Offset.unsafe(offset))
      val record = ActionRecord(mark, partitionOffset)
      val marker = Marker(mark.id, partitionOffset)
      (marker, record)
    }

    val appendRecords = for {
      pointer <- pointers
    } yield {
      val range = SeqRange.unsafe(pointer.seqNr)
      val header = ActionHeader.Append(
        range = range,
        origin = none,
        version = Version.current.some,
        payloadType = PayloadType.Json,
        metadata = HeaderMetadata.empty,
      )
      val action = Action.Append(key, timestamp, header, ByteVector.empty, Headers.empty)
      ActionRecord(action, PartitionOffset(offset = Offset.unsafe(pointer.offset)))
    }
    val records = appendRecords :+ markRecord

    val consumeActionRecords: ConsumeActionRecords[StateT] = { (_: Key, _: Partition, from: Offset) =>
      {
        val actionRecords = StateT { state =>
          val records = state
            .records
            .dropWhile { _.offset < from }
          records match {
            case h :: t => (state.copy(records = t), Stream[StateT].single(h))
            case _ => (state, Stream[StateT].empty[ActionRecord[Action]])
          }
        }
        Stream.repeat(actionRecords).flatten
      }
    }

    def seqNrAndOffset(action: Action.Append, partitionOffset: PartitionOffset) = {
      for {
        seqNr <- action.range.toNel.toList
      } yield {
        (seqNr.value, partitionOffset.offset.value)
      }
    }

    val actionRecords =
      StreamActionRecords[StateT](key, SeqNr.min, marker, replicated.map(Offset.unsafe(_)), consumeActionRecords)
    val (_, result) = actionRecords(offset.map(Offset.unsafe(_)))
      .collect { case ActionRecord(a: Action.Append, partitionOffset) => seqNrAndOffset(a, partitionOffset) }
      .toList
      .run(State(records))
      .get
    result.flatten
  }

  final case class Pointer(seqNr: Long, offset: Long)

  final case class State(records: List[ActionRecord[Action]])

  type StateT[A] = cats.data.StateT[Try, State, A]

  object StateT {

    def apply[A](f: State => (State, A)): StateT[A] = {
      cats.data.StateT[Try, State, A] { state => Success(f(state)) }
    }
  }
}
