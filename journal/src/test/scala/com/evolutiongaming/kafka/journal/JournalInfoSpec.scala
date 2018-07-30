package com.evolutiongaming.kafka.journal

import com.evolutiongaming.kafka.journal.Action.{Header => A}
import com.evolutiongaming.kafka.journal.SeqNr.Helper._
import com.evolutiongaming.kafka.journal.{JournalInfo => J}
import org.scalatest.{FunSuite, Matchers}

class JournalInfoSpec extends FunSuite with Matchers {

  test("Empty apply Append") {
    J.Empty(A.Append(SeqRange(1, 2))) shouldEqual J.NonEmpty(2.toSeqNr)
  }

  test("Empty apply Delete") {
    J.Empty(A.Delete(10.toSeqNr)) shouldEqual J.DeleteTo(10.toSeqNr)
  }

  test("Empty apply Mark") {
    J.Empty(A.Mark("id")) shouldEqual J.Empty
  }

  test("NonEmpty apply Append") {
    J.NonEmpty(1.toSeqNr)(A.Append(SeqRange(2, 3))) shouldEqual J.NonEmpty(3.toSeqNr)
    J.NonEmpty(2.toSeqNr, Some(1.toSeqNr))(A.Append(SeqRange(3, 4))) shouldEqual J.NonEmpty(4.toSeqNr, Some(1.toSeqNr))
  }

  test("NonEmpty apply Delete") {
    J.NonEmpty(2.toSeqNr)(A.Delete(3.toSeqNr)) shouldEqual J.NonEmpty(2.toSeqNr, Some(2.toSeqNr))
    J.NonEmpty(2.toSeqNr)(A.Delete(1.toSeqNr)) shouldEqual J.NonEmpty(2.toSeqNr, Some(1.toSeqNr))
    J.NonEmpty(2.toSeqNr, Some(1.toSeqNr))(A.Delete(3.toSeqNr)) shouldEqual J.NonEmpty(2.toSeqNr, Some(2.toSeqNr))
    J.NonEmpty(2.toSeqNr, Some(2.toSeqNr))(A.Delete(1.toSeqNr)) shouldEqual J.NonEmpty(2.toSeqNr, Some(2.toSeqNr))
  }

  test("NonEmpty apply Mark") {
    J.NonEmpty(2.toSeqNr)(A.Mark("id")) shouldEqual J.NonEmpty(2.toSeqNr)
  }

  test("DeleteTo apply Append") {
    J.DeleteTo(1.toSeqNr)(A.Append(SeqRange(1, 2))) shouldEqual J.NonEmpty(2.toSeqNr)
    J.DeleteTo(10.toSeqNr)(A.Append(SeqRange(1, 2))) shouldEqual J.NonEmpty(2.toSeqNr)
    J.DeleteTo(10.toSeqNr)(A.Append(SeqRange(2, 3))) shouldEqual J.NonEmpty(3.toSeqNr, Some(1.toSeqNr))
  }

  test("DeleteTo apply Delete") {
    J.DeleteTo(1.toSeqNr)(A.Delete(2.toSeqNr)) shouldEqual J.DeleteTo(2.toSeqNr)
    J.DeleteTo(2.toSeqNr)(A.Delete(1.toSeqNr)) shouldEqual J.DeleteTo(2.toSeqNr)
  }

  test("DeleteTo apply Mark") {
    J.DeleteTo(1.toSeqNr)(A.Mark("id")) shouldEqual J.DeleteTo(1.toSeqNr)
  }
}
