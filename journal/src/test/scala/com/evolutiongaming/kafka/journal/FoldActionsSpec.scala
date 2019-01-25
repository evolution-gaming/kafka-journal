package com.evolutiongaming.kafka.journal

import java.time.Instant

import com.evolutiongaming.skafka.Offset
import org.scalatest.{FunSuite, Matchers}

import scala.collection.immutable.Queue

class FoldActionsSpec extends FunSuite with Matchers {

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

    val withPollActions = WithPollActionsOneByOne[cats.Id](records.to[Queue])

    val foldActions = FoldActions[cats.Id](key, SeqNr.Min, marker, replicated, withPollActions)
    foldActions(offset).collect { case a: Action.Append => a.range.seqNrs.toList }.toList.flatten.map(_.value)
  }

  case class Pointer(seqNr: Long, offset: Offset)
}
