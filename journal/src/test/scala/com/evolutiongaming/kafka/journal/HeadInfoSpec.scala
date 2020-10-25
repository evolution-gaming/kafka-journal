package com.evolutiongaming.kafka.journal

import cats.syntax.all._
import com.evolutiongaming.skafka.Offset
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class HeadInfoSpec extends AnyFunSuite with Matchers {

  test("Empty apply Append") {
    HeadInfo.Empty(append(1, 2), Offset.min) shouldEqual appendInfo(0, 2)
  }

  test("Empty apply Delete") {
    HeadInfo.Empty(delete(10), Offset.min) shouldEqual deleteInfo(10)
  }

  test("Empty apply Purge") {
    HeadInfo.Empty(purge, Offset.min) shouldEqual HeadInfo.Purge
  }

  test("Empty apply Mark") {
    HeadInfo.Empty(mark, Offset.min) shouldEqual HeadInfo.Empty
  }

  test("NonEmpty apply Append") {
    appendInfo(0, 1)(append(2, 3), Offset.unsafe(1)) shouldEqual appendInfo(0, 3)
    appendInfo(0, 2, 1.some)(append(3, 4), Offset.unsafe(1)) shouldEqual appendInfo(0, 4, 1.some)
  }

  test("NonEmpty apply Delete") {
    appendInfo(0, 2)(delete(3), Offset.unsafe(1)) shouldEqual appendInfo(0, 2, 2.some)
    appendInfo(0, 2)(delete(1), Offset.unsafe(1)) shouldEqual appendInfo(0, 2, 1.some)
    appendInfo(0, 2, 1.some)(delete(3), Offset.unsafe(1)) shouldEqual appendInfo(0, 2, 2.some)
    appendInfo(0, 2, 2.some)(delete(1), Offset.unsafe(1)) shouldEqual appendInfo(0, 2, 2.some)
  }

  test("NonEmpty apply Purge") {
    appendInfo(0, 2)(purge, Offset.unsafe(1)) shouldEqual HeadInfo.Purge
  }

  test("NonEmpty apply Mark") {
    appendInfo(0, 2)(mark, Offset.unsafe(1)) shouldEqual appendInfo(0, 2)
  }

  test("Delete apply Append") {
    deleteInfo(1)(append(1, 2), Offset.unsafe(1)) shouldEqual appendInfo(1, 2)
    deleteInfo(10)(append(1, 2), Offset.unsafe(1)) shouldEqual appendInfo(1, 2)
    deleteInfo(10)(append(2, 3), Offset.unsafe(1)) shouldEqual appendInfo(1, 3, 1.some)
  }

  test("Delete apply Delete") {
    deleteInfo(1)(delete(2), Offset.unsafe(1)) shouldEqual deleteInfo(2)
    deleteInfo(2)(delete(1), Offset.unsafe(1)) shouldEqual deleteInfo(2)
  }

  test("Delete apply Purge") {
    deleteInfo(1)(purge, Offset.unsafe(1)) shouldEqual HeadInfo.Purge
  }

  test("Delete apply Mark") {
    deleteInfo(1)(mark, Offset.unsafe(1)) shouldEqual deleteInfo(1)
  }

  private def append(from: Int, to: Int) = {
    ActionHeader.Append(
      range = SeqRange.unsafe(from, to),
      origin = None,
      payloadType = PayloadType.Json,
      metadata = HeaderMetadata.empty)
  }

  private def delete(seqNr: Int) = {
    val deleteTo = SeqNr.unsafe(seqNr).toDeleteTo
    ActionHeader.Delete(deleteTo, None)
  }

  private def mark = ActionHeader.Mark("id", None)

  private def purge = ActionHeader.Purge(None)

  private def deleteInfo(seqNr: Int) = {
    val deleteTo = SeqNr.unsafe(seqNr).toDeleteTo
    HeadInfo.Delete(deleteTo)
  }

  private def appendInfo(offset: Int, seqNr: Int, deleteTo: Option[Int] = None) = {
    HeadInfo.Append(
      seqNr = SeqNr.unsafe(seqNr),
      deleteTo = deleteTo.map { deleteTo => SeqNr.unsafe(deleteTo).toDeleteTo },
      offset = Offset.unsafe(offset))
  }
}
