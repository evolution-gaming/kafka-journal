package com.evolutiongaming.kafka.journal

import com.evolutiongaming.kafka.journal.Action.{Header => A}
import com.evolutiongaming.kafka.journal.{ActionBatch => B}
import org.scalatest.{FunSuite, Matchers}

class ActionBatchSpec extends FunSuite with Matchers {

  test("Empty apply Append") {
    B.Empty(A.Append(SeqRange(1, 2))) shouldEqual B.NonEmpty(2)
  }

  test("Empty apply Delete") {
    B.Empty(A.Delete(10)) shouldEqual B.DeleteTo(10)
  }

  test("Empty apply Mark") {
    B.Empty(A.Mark("id")) shouldEqual B.Empty
  }

  test("NonEmpty apply Append") {
    B.NonEmpty(1)(A.Append(SeqRange(2, 3))) shouldEqual B.NonEmpty(3)
    B.NonEmpty(2, Some(1))(A.Append(SeqRange(3, 4))) shouldEqual B.NonEmpty(4, Some(1))
  }

  test("NonEmpty apply Delete") {
    B.NonEmpty(2)(A.Delete(3)) shouldEqual B.NonEmpty(2, Some(2))
    B.NonEmpty(2)(A.Delete(1)) shouldEqual B.NonEmpty(2, Some(1))
    B.NonEmpty(2, Some(1))(A.Delete(3)) shouldEqual B.NonEmpty(2, Some(2))
    B.NonEmpty(2, Some(2))(A.Delete(1)) shouldEqual B.NonEmpty(2, Some(2))
  }

  test("NonEmpty apply Mark") {
    B.NonEmpty(2)(A.Mark("id")) shouldEqual B.NonEmpty(2)
  }

  test("DeleteTo apply Append") {
    B.DeleteTo(1)(A.Append(SeqRange(1, 2))) shouldEqual B.NonEmpty(2)
    B.DeleteTo(10)(A.Append(SeqRange(1, 2))) shouldEqual B.NonEmpty(2)
    B.DeleteTo(10)(A.Append(SeqRange(2, 3))) shouldEqual B.NonEmpty(3, Some(1))
  }

  test("DeleteTo apply Delete") {
    B.DeleteTo(1)(A.Delete(2)) shouldEqual B.DeleteTo(2)
    B.DeleteTo(2)(A.Delete(1)) shouldEqual B.DeleteTo(2)
  }

  test("DeleteTo apply Mark") {
    B.DeleteTo(1)(A.Mark("id")) shouldEqual B.DeleteTo(1)
  }
}
