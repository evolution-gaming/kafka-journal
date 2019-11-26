package com.evolutiongaming.kafka.journal.replicator

import cats.data.{NonEmptyList => Nel, NonEmptyMap => Nem}
import cats.implicits._
import cats.{Applicative, ~>}
import com.evolutiongaming.kafka.journal.ConsRecord
import com.evolutiongaming.skafka.{Offset, Partition}


trait TopicFlow[F[_]] {

  def assign(partitions: Nel[Partition]): F[Unit]

  def apply(records: Nem[Partition, Nel[ConsRecord]]): F[Map[Partition, Offset]]

  def revoke(partitions: Nel[Partition]): F[Unit]
}

object TopicFlow {

  def empty[F[_] : Applicative]: TopicFlow[F] = new TopicFlow[F] {

    def assign(partitions: Nel[Partition]) = ().pure[F]

    def apply(records: Nem[Partition, Nel[ConsRecord]]) = Map.empty[Partition, Offset].pure[F]

    def revoke(partitions: Nel[Partition]) = ().pure[F]
  }


  implicit class TopicFlowOps[F[_]](val self: TopicFlow[F]) extends AnyVal {

    def mapK[G[_]](f: F ~> G): TopicFlow[G] = new TopicFlow[G] {

      def assign(partitions: Nel[Partition]) = f(self.assign(partitions))

      def apply(records: Nem[Partition, Nel[ConsRecord]]) = f(self(records))

      def revoke(partitions: Nel[Partition]) = f(self.revoke(partitions))
    }
  }
}