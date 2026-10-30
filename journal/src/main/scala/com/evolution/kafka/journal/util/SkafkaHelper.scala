package com.evolution.kafka.journal.util

import cats.syntax.all.*
import cats.{Applicative, ApplicativeThrow, Hash}
import com.evolutiongaming.catshelper.ApplicativeThrowable
import com.evolutiongaming.skafka
import com.evolutiongaming.skafka.{Bytes, Offset, Partition, Topic}
import scodec.bits.ByteVector

// TODO move to skafka & scassandra
object SkafkaHelper {

  implicit def skafkaFromBytes[F[_]: Applicative]: skafka.FromBytes[F, ByteVector] = {
    new skafka.FromBytes[F, ByteVector] {

      def apply(bytes: Bytes, topic: Topic): F[ByteVector] = ByteVector.view(bytes).pure[F]
    }
  }

  implicit def skafkaToBytes[F[_]: Applicative]: skafka.ToBytes[F, ByteVector] = new skafka.ToBytes[F, ByteVector] {

    def apply(a: ByteVector, topic: Topic): F[Bytes] = a.toArray.pure[F]
  }

  implicit class OffsetOps(val self: Offset) extends AnyVal {

    def inc[F[_]: ApplicativeThrowable]: F[Offset] = Offset.of[F](self.value + 1)

    def dec[F[_]: ApplicativeThrowable]: F[Offset] = Offset.of[F](self.value - 1)
  }

  implicit val hashPartition: Hash[Partition] = Hash.by { _.value }

  implicit class PartitionOps(val self: Partition) extends AnyVal {
    def inc[F[_]: ApplicativeThrow]: F[Partition] = Partition.of[F](self.value + 1)
  }
}
