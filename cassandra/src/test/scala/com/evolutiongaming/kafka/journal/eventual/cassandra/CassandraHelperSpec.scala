package com.evolutiongaming.kafka.journal.eventual.cassandra

import CassandraHelper.*
import org.scalatest.funsuite.AnyFunSuite

class CassandraHelperSpec extends AnyFunSuite {

  test("wasApplied returns true when a statement was applied") {
    val row = new RowStub {
      override def getBool(name: String): Boolean = name match {
        case "[applied]" => true
        case _ => throw new IllegalArgumentException
      }
    }
    assert(row.wasApplied)
  }

  test("wasApplied returns false when a statement was not applied") {
    val row = new RowStub {
      override def getBool(name: String): Boolean = name match {
        case "[applied]" => false
        case _ => throw new IllegalArgumentException
      }
    }
    assert(!row.wasApplied)
  }

  test("wasApplied throws an exception when a statement was not conditional") {
    val row = new RowStub {
      override def getBool(name: String): Boolean = name match {
        case "[applied]" => throw new IllegalArgumentException
        case _ => fail("[applied] column was not requested")
      }
    }
    assertThrows[IllegalArgumentException](row.wasApplied)
  }

}
