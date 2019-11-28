package com.evolutiongaming.kafka.journal


import org.scalatest.{FunSuite, Matchers}

class HeadInfoSpec extends FunSuite with Matchers {

  test("Empty apply Append") {
    HeadInfo.Empty(append(1, 2)) shouldEqual appendInfo(2)
  }

  test("Empty apply Delete") {
    HeadInfo.Empty(delete(10)) shouldEqual deleteInfo(10)
  }

  test("Empty apply Purge") {
    HeadInfo.Empty(purge) shouldEqual HeadInfo.Purge
  }

  test("Empty apply Mark") {
    HeadInfo.Empty(mark) shouldEqual HeadInfo.Empty
  }

  test("NonEmpty apply Append") {
    appendInfo(1)(append(2, 3)) shouldEqual appendInfo(3)
    appendInfo(2, Some(1))(append(3, 4)) shouldEqual appendInfo(4, Some(1))
  }

  test("NonEmpty apply Delete") {
    appendInfo(2)(delete(3)) shouldEqual appendInfo(2, Some(2))
    appendInfo(2)(delete(1)) shouldEqual appendInfo(2, Some(1))
    appendInfo(2, Some(1))(delete(3)) shouldEqual appendInfo(2, Some(2))
    appendInfo(2, Some(2))(delete(1)) shouldEqual appendInfo(2, Some(2))
  }

  test("NonEmpty apply Purge") {
    appendInfo(2)(purge) shouldEqual HeadInfo.Purge
  }

  test("NonEmpty apply Mark") {
    appendInfo(2)(mark) shouldEqual appendInfo(2)
  }

  test("DeleteTo apply Append") {
    deleteInfo(1)(append(1, 2)) shouldEqual appendInfo(2)
    deleteInfo(10)(append(1, 2)) shouldEqual appendInfo(2)
    deleteInfo(10)(append(2, 3)) shouldEqual appendInfo(3, Some(1))
  }

  test("DeleteTo apply Delete") {
    deleteInfo(1)(delete(2)) shouldEqual deleteInfo(2)
    deleteInfo(2)(delete(1)) shouldEqual deleteInfo(2)
  }

  test("DeleteTo apply Purge") {
    deleteInfo(1)(purge) shouldEqual HeadInfo.Purge
  }

  test("DeleteTo apply Mark") {
    deleteInfo(1)(mark) shouldEqual deleteInfo(1)
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

  private def appendInfo(seqNr: Int, deleteTo: Option[Int] = None) = {
    HeadInfo.Append(
      seqNr = SeqNr.unsafe(seqNr),
      deleteTo.map { deleteTo => SeqNr.unsafe(deleteTo) })
  }
}
