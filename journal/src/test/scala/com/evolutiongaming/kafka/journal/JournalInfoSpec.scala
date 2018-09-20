package com.evolutiongaming.kafka.journal

import java.time.Instant

import org.scalatest.{FunSuite, Matchers}

class JournalInfoSpec extends FunSuite with Matchers {

  test("Empty apply Append") {
    Empty(append(1, 2)) shouldEqual nonEmpty(2)
  }

  test("Empty apply Delete") {
    Empty(delete(10)) shouldEqual deleted(10)
  }

  test("Empty apply Mark") {
    Empty(mark) shouldEqual Empty
  }

  test("NonEmpty apply Append") {
    nonEmpty(1)(append(2, 3)) shouldEqual nonEmpty(3)
    nonEmpty(2, Some(1))(append(3, 4)) shouldEqual nonEmpty(4, Some(1))
  }

  test("NonEmpty apply Delete") {
    nonEmpty(2)(delete(3)) shouldEqual nonEmpty(2, Some(2))
    nonEmpty(2)(delete(1)) shouldEqual nonEmpty(2, Some(1))
    nonEmpty(2, Some(1))(delete(3)) shouldEqual nonEmpty(2, Some(2))
    nonEmpty(2, Some(2))(delete(1)) shouldEqual nonEmpty(2, Some(2))
  }

  test("NonEmpty apply Mark") {
    nonEmpty(2)(mark) shouldEqual nonEmpty(2)
  }

  test("DeleteTo apply Append") {
    deleted(1)(append(1, 2)) shouldEqual nonEmpty(2)
    deleted(10)(append(1, 2)) shouldEqual nonEmpty(2)
    deleted(10)(append(2, 3)) shouldEqual nonEmpty(3, Some(1))
  }

  test("DeleteTo apply Delete") {
    deleted(1)(delete(2)) shouldEqual deleted(2)
    deleted(2)(delete(1)) shouldEqual deleted(2)
  }

  test("DeleteTo apply Mark") {
    deleted(1)(mark) shouldEqual deleted(1)
  }

  private val timestamp = Instant.now()
  private val keyEmpty = Key(id = "id", topic = "topic")

  private def append(from: Int, to: Int) = {
    val range = SeqRange(SeqNr(from.toLong /*TODO try to avoid .toLong ?*/), SeqNr(to.toLong))
    Action.Append(keyEmpty, timestamp, None, range, Bytes.Empty)
  }

  private def delete(seqNr: Int) = {
    Action.Delete(keyEmpty, timestamp, None, SeqNr(seqNr.toLong))
  }

  private def mark = Action.Mark(keyEmpty, timestamp, None, "id")

  private def deleted(seqNr: Int) = {
    JournalInfo.Deleted(SeqNr(seqNr.toLong /*TODO try to avoid .toLong ?*/))
  }

  private def nonEmpty(seqNr: Int, deleteTo: Option[Int] = None) = {
    JournalInfo.NonEmpty(SeqNr(seqNr.toLong), deleteTo.map(x => SeqNr(x.toLong)))
  }

  private def Empty = JournalInfo.Empty
}
