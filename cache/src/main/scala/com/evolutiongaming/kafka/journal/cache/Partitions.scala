package com.evolutiongaming.kafka.journal.cache

import cats.Monad
import cats.implicits._

trait Partitions[-K, +V] {

  def get(key: K): V

  def values: List[V]
}

object Partitions {

  type Partition = Int


  def const[K, V](value: V): Partitions[K, V] = new Partitions[K, V] {

    def get(key: K) = value

    val values = List(value)
  }

  def of[F[_] : Monad, K, V](nrOfPartitions: Int, valueOf: Partition => F[V], hashCodeOf: K => Int): F[Partitions[K, V]] = {

    def apply(nrOfPartitions: Int) = {

      val partitions = (0 until nrOfPartitions).toList.foldLeftM(List.empty[V]) { (a, b) =>
        for {
          value <- valueOf(b)
        } yield {
          value :: a
        }
      }
      for {
        partitions <- partitions
      } yield {
        Partitions(partitions.reverse.toVector, hashCodeOf)
      }
    }

    if (nrOfPartitions <= 1) valueOf(0).map(const) else apply(nrOfPartitions)
  }


  private def apply[K, V](partitions: Vector[V], hashCodeOf: K => Int): Partitions[K, V] = {

    val nrOfPartitions = partitions.size

    new Partitions[K, V] {

      def get(key: K) = {
        val hashCode = hashCodeOf(key)
        val partition = math.abs(hashCode % nrOfPartitions)
        partitions(partition)
      }

      val values = partitions.toList
    }
  }
}