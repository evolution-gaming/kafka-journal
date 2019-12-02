package com.evolutiongaming.kafka.journal

import cats.implicits._
import com.evolutiongaming.skafka.Offset
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class HeadInfoSpec extends AnyFunSuite with Matchers {

  test("Empty apply Append") {
    HeadInfo.Empty(append(1, 2), 0) shouldEqual appendInfo(0, 2)
  }

  test("Empty apply Delete") {
    HeadInfo.Empty(delete(10), 0) shouldEqual deleteInfo(10)
  }

  test("Empty apply Purge") {
    HeadInfo.Empty(purge, 0) shouldEqual HeadInfo.Purge
  }

  test("Empty apply Mark") {
    HeadInfo.Empty(mark, 0) shouldEqual HeadInfo.Empty
  }

  test("NonEmpty apply Append") {
    appendInfo(0, 1)(append(2, 3), 1) shouldEqual appendInfo(0, 3)
    appendInfo(0, 2, 1.some)(append(3, 4), 1) shouldEqual appendInfo(0, 4, 1.some)
  }

  test("NonEmpty apply Delete") {
    appendInfo(0, 2)(delete(3), 1) shouldEqual appendInfo(0, 2, 2.some)
    appendInfo(0, 2)(delete(1), 1) shouldEqual appendInfo(0, 2, 1.some)
    appendInfo(0, 2, 1.some)(delete(3), 1) shouldEqual appendInfo(0, 2, 2.some)
    appendInfo(0, 2, 2.some)(delete(1), 1) shouldEqual appendInfo(0, 2, 2.some)
  }

  test("NonEmpty apply Purge") {
    appendInfo(0, 2)(purge, 1) shouldEqual HeadInfo.Purge
  }

  test("NonEmpty apply Mark") {
    appendInfo(0, 2)(mark, 1) shouldEqual appendInfo(0, 2)
  }

  test("Delete apply Append") {
    deleteInfo(1)(append(1, 2), 1) shouldEqual appendInfo(1, 2)
    deleteInfo(10)(append(1, 2), 1) shouldEqual appendInfo(1, 2)
    deleteInfo(10)(append(2, 3), 1) shouldEqual appendInfo(1, 3, 1.some)
  }

  test("Delete apply Delete") {
    deleteInfo(1)(delete(2), 1) shouldEqual deleteInfo(2)
    deleteInfo(2)(delete(1), 1) shouldEqual deleteInfo(2)
  }

  test("Delete apply Purge") {
    deleteInfo(1)(purge, 1) shouldEqual HeadInfo.Purge
  }

  test("Delete apply Mark") {
    deleteInfo(1)(mark, 1) shouldEqual deleteInfo(1)
  }

  private def append(from: Int, to: Int) = {
    val range = SeqRange.unsafe(from, to)
    val metadata = RecordMetadata.empty
    ActionHeader.Append(
      range = range,
      origin = None,
      payloadType = PayloadType.Json,
      metadata = metadata,
      expireAfter = None)
  }

  private def delete(seqNr: Int) = {
    ActionHeader.Delete(SeqNr.unsafe(seqNr), None)
  }

  private def mark = ActionHeader.Mark("id", None)

  private def purge = ActionHeader.Purge(None)

  private def deleteInfo(seqNr: Int) = {
    HeadInfo.Delete(SeqNr.unsafe(seqNr))
  }

  private def appendInfo(offset: Offset, seqNr: Int, deleteTo: Option[Int] = None) = {
    HeadInfo.Append(
      seqNr = SeqNr.unsafe(seqNr),
      deleteTo = deleteTo.map { deleteTo => SeqNr.unsafe(deleteTo) },
      offset = offset)
  }
}
