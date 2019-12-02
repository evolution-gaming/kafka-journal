package com.evolutiongaming.kafka.journal.util

import cats.data.{NonEmptyMap => Nem}
import cats.implicits._
import com.evolutiongaming.kafka.journal.util.CollectionHelper._
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import scala.collection.immutable.SortedMap

class CollectionHelperTest extends AnyFunSuite with Matchers {

  test("toSortedMap") {
    List.empty[(Int, Int)].toSortedMap shouldEqual SortedMap.empty[Int, Int]
    Map((0, 0), (1, 1), (2, 2)).toSortedMap shouldEqual SortedMap((0, 0), (1, 1), (2, 2))
  }

  test("toNem") {
    Map((0, 1)).toNem shouldEqual Nem.of((0, 1)).some
    Map.empty[Int, Int].toNem shouldEqual none
  }
}
