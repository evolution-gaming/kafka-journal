package com.evolutiongaming.kafka.journal

import com.evolutiongaming.kafka.journal.Action.{Header => A}
import com.evolutiongaming.kafka.journal.{JournalInfo => J}
import org.scalatest.{FunSuite, Matchers}

class JournalInfoSpec extends FunSuite with Matchers {

  test("Empty apply Append") {
    J.Empty(A.Append(SeqRange(1, 2))) shouldEqual J.NonEmpty(2)
  }

  test("Empty apply Delete") {
    J.Empty(A.Delete(10)) shouldEqual J.DeleteTo(10)
  }

  test("Empty apply Mark") {
    J.Empty(A.Mark("id")) shouldEqual J.Empty
  }

  test("NonEmpty apply Append") {
    J.NonEmpty(1)(A.Append(SeqRange(2, 3))) shouldEqual J.NonEmpty(3)
    J.NonEmpty(2, Some(1))(A.Append(SeqRange(3, 4))) shouldEqual J.NonEmpty(4, Some(1))
  }

  test("NonEmpty apply Delete") {
    J.NonEmpty(2)(A.Delete(3)) shouldEqual J.NonEmpty(2, Some(2))
    J.NonEmpty(2)(A.Delete(1)) shouldEqual J.NonEmpty(2, Some(1))
    J.NonEmpty(2, Some(1))(A.Delete(3)) shouldEqual J.NonEmpty(2, Some(2))
    J.NonEmpty(2, Some(2))(A.Delete(1)) shouldEqual J.NonEmpty(2, Some(2))
  }

  test("NonEmpty apply Mark") {
    J.NonEmpty(2)(A.Mark("id")) shouldEqual J.NonEmpty(2)
  }

  test("DeleteTo apply Append") {
    J.DeleteTo(1)(A.Append(SeqRange(1, 2))) shouldEqual J.NonEmpty(2)
    J.DeleteTo(10)(A.Append(SeqRange(1, 2))) shouldEqual J.NonEmpty(2)
    J.DeleteTo(10)(A.Append(SeqRange(2, 3))) shouldEqual J.NonEmpty(3, Some(1))
  }

  test("DeleteTo apply Delete") {
    J.DeleteTo(1)(A.Delete(2)) shouldEqual J.DeleteTo(2)
    J.DeleteTo(2)(A.Delete(1)) shouldEqual J.DeleteTo(2)
  }

  test("DeleteTo apply Mark") {
    J.DeleteTo(1)(A.Mark("id")) shouldEqual J.DeleteTo(1)
  }
}
