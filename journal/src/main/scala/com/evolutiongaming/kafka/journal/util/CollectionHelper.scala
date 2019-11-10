package com.evolutiongaming.kafka.journal.util

import cats.Order
import cats.data.{NonEmptyMap => Nem}

import scala.collection.immutable.{SortedMap, Iterable}

object CollectionHelper {

  implicit class IterableOpsCollectionHelper[K, V](val self: Iterable[(K, V)]) extends AnyVal {

    def toSortedMap(implicit order: Order[K]): SortedMap[K, V] = {
      implicit val ordering = order.toOrdering
      val builder = SortedMap.newBuilder[K, V]
      val result = builder ++= self
      result.result()
    }

    def toNem(implicit order: Order[K]): Option[Nem[K, V]] = {
      val sortedMap = self.toSortedMap
      Nem.fromMap(sortedMap)
    }
  }
}
