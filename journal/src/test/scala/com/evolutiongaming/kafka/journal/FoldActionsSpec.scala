package com.evolutiongaming.kafka.journal

import java.time.Instant

import com.evolutiongaming.kafka.journal.FoldWhileHelper._
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
    val (marker, markRecord) = {
      val offset = pointers.lastOption.fold(1L) { _.offset + 1 }
      val mark = Action.Mark("mark", timestamp)
      val record = ActionRecord(Action.Mark("mark", timestamp), offset)
      val partitionOffset = PartitionOffset(partition = 0, offset = offset)
      val marker = Marker(mark.header.id, partitionOffset)
      (marker, record)
    }
    val key = Key(topic = "topic", id = "id")

    val appendRecords = for {
      pointer <- pointers
    } yield {
      val action = Action.Append(SeqRange(SeqNr(pointer.seqNr)), timestamp, Bytes.Empty)
      ActionRecord(action, pointer.offset)
    }
    val records = appendRecords :+ markRecord

    val withReadActions = WithReadActionsOneByOne(records.to[Queue])

    val foldActions = FoldActions(key, SeqNr.Min, marker, replicated, withReadActions)
    val seqNr = foldActions(offset, List.empty[SeqNr]) { (s, action) =>
      action match {
        case action: Action.Append => (action.range.seqNrs.toList ::: s).continue
        case action: Action.Delete => s.continue
      }
    }
    seqNr.value().get.get.reverse.map(_.value)
  }

  case class Pointer(seqNr: Long, offset: Offset)
}
