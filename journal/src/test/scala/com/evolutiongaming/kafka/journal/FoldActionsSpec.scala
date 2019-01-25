package com.evolutiongaming.kafka.journal

import java.time.Instant

import cats.data.StateT
import com.evolutiongaming.skafka.{Offset, Partition}
import org.scalatest.{FunSuite, Matchers}

class FoldActionsSpec extends FunSuite with Matchers {

  import FoldActionsSpec._

  test("no offsets") {
    val records = List(
      Pointer(seqNr = 1l, offset = 11l),
      Pointer(seqNr = 2l, offset = 12l),
      Pointer(seqNr = 3l, offset = 13l))
    val result = seqNrs(None, None, records)
    result shouldEqual List(1l, 2l, 3l)
  }

  test("replicated offset") {
    val records = List(
      Pointer(seqNr = 1l, offset = 11l),
      Pointer(seqNr = 2l, offset = 12l),
      Pointer(seqNr = 3l, offset = 13l))
    val result = seqNrs(Some(11l), None, records)
    result shouldEqual List(2l, 3l)
  }

  test("replicated and queried offsets") {
    val records = List(
      Pointer(seqNr = 1l, offset = 11l),
      Pointer(seqNr = 2l, offset = 12l),
      Pointer(seqNr = 3l, offset = 13l))
    val result = seqNrs(Some(11l), Some(12l), records)
    result shouldEqual List(3l)
  }

  test("queried offsets covers all kafka records") {
    val records = List(
      Pointer(seqNr = 1l, offset = 1l),
      Pointer(seqNr = 2l, offset = 2l),
      Pointer(seqNr = 3l, offset = 3l))
    val result = seqNrs(Some(11l), Some(13l), records)
    result shouldEqual Nil
  }

  private def seqNrs(
    replicated: Option[Offset],
    offset: Option[Offset],
    pointers: List[Pointer]) = {

    val timestamp = Instant.now()
    val key = Key(topic = "topic", id = "id")

    val (marker, markRecord) = {
      val offset = pointers.lastOption.fold(1l) { _.offset + 1 }
      val mark = Action.Mark(key, timestamp, None, "mark")
      val partitionOffset = PartitionOffset(offset = offset)
      val record = ActionRecord(mark, partitionOffset)
      val marker = Marker(mark.id, partitionOffset)
      (marker, record)
    }

    val appendRecords = for {
      pointer <- pointers
    } yield {
      val range = SeqRange(SeqNr(pointer.seqNr))
      val action = Action.Append(key, timestamp, None, range, PayloadType.Json, Payload.Binary.Empty)
      ActionRecord(action, PartitionOffset(offset = pointer.offset))
    }
    val records = appendRecords :+ markRecord

    val withPollActions = new WithPollActions[StateM] {
      def apply[A](key: Key, partition: Partition, offset: Option[Offset])(f: PollActions[StateM] => StateM[A]) = {
        val pollActions = new PollActions[StateM] {
          def apply() = StateS { s =>
            val records = offset.fold(s.records) { offset => s.records.dropWhile(_.offset <= offset) }
            records match {
              case h :: t => (s.copy(records = t), List(h))
              case _ => (s, Nil)
            }
          }
        }
        f(pollActions)
      }
    }

    val foldActions = FoldActions[StateM](key, SeqNr.Min, marker, replicated, withPollActions)
    val (_, result) = foldActions(offset).collect { case a: Action.Append => a.range.seqNrs.toList }.toList.run(State(records))
    result.flatten.map(_.value)
  }

  case class Pointer(seqNr: Long, offset: Offset)
}

object FoldActionsSpec {

  final case class State(records: List[ActionRecord[Action]])

  type StateM[A] = StateT[cats.Id, State, A]

  object StateS {

    def apply[A](f: State => (State, A)): StateM[A] = StateT[cats.Id, State, A](f)
  }
}