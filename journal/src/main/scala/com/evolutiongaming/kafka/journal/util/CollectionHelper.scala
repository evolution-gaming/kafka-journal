package com.evolutiongaming.kafka.journal.util

import cats.Order
import cats.data.{NonEmptyMap => Nem}

import scala.collection.immutable.{Iterable, SortedMap}

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


  implicit class NemOpsCollectionHelper[K, V](val self: Nem[K, V]) extends AnyVal {

    def mapKV[A: Order, B](f: (K, V) => (A, B)): Nem[A, B] = {
      self
        .toSortedMap.map { case (k, v) => f(k, v) }
        .toNem
        .get
    }
  }
}
