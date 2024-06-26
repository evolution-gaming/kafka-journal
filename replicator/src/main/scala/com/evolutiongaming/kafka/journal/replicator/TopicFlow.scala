package com.evolutiongaming.kafka.journal.replicator

import cats.data.{NonEmptyList => Nel, NonEmptyMap => Nem, NonEmptySet => Nes}
import cats.syntax.all._
import cats.{Applicative, ~>}
import com.evolutiongaming.kafka.journal.ConsRecord
import com.evolutiongaming.skafka.{Offset, Partition}

trait TopicFlow[F[_]] {

  def assign(partitions: Nes[Partition]): F[Unit]

  def apply(records: Nem[Partition, Nel[ConsRecord]]): F[Map[Partition, Offset]]

  def revoke(partitions: Nes[Partition]): F[Unit]

  def lose(partitions: Nes[Partition]): F[Unit]
}

object TopicFlow {

  def empty[F[_]: Applicative]: TopicFlow[F] = new TopicFlow[F] {

    def assign(partitions: Nes[Partition]) = ().pure[F]

    def apply(records: Nem[Partition, Nel[ConsRecord]]) = Map.empty[Partition, Offset].pure[F]

    def revoke(partitions: Nes[Partition]) = ().pure[F]

    def lose(partitions: Nes[Partition]) = ().pure[F]
  }

  implicit class TopicFlowOps[F[_]](val self: TopicFlow[F]) extends AnyVal {

    def mapK[G[_]](f: F ~> G): TopicFlow[G] = new TopicFlow[G] {

      def assign(partitions: Nes[Partition]) = f(self.assign(partitions))

      def apply(records: Nem[Partition, Nel[ConsRecord]]) = f(self(records))

      def revoke(partitions: Nes[Partition]) = f(self.revoke(partitions))

      def lose(partitions: Nes[Partition]) = f(self.lose(partitions))
    }
  }
}
