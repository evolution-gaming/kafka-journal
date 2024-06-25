package com.evolutiongaming.kafka.journal.util

import cats.syntax.all._
import cats.{Applicative, ApplicativeThrow, Hash}
import com.datastax.driver.core.{GettableByNameData, SettableData}
import com.evolutiongaming.catshelper.ApplicativeThrowable
import com.evolutiongaming.scassandra.syntax._
import com.evolutiongaming.scassandra.{DecodeByName, DecodeRow, EncodeByName, EncodeRow}
import com.evolutiongaming.skafka
import com.evolutiongaming.skafka.{Bytes, Offset, Partition, Topic}
import scodec.bits.ByteVector

import scala.util.Try

object SkafkaHelper {

  implicit def skafkaFromBytes[F[_]: Applicative]: skafka.FromBytes[F, ByteVector] = { (a: Bytes, _: Topic) =>
    ByteVector.view(a).pure[F]
  }

  implicit def skafkaToBytes[F[_]: Applicative]: skafka.ToBytes[F, ByteVector] = { (a: ByteVector, _: Topic) =>
    a.toArray.pure[F]
  }

  implicit class OffsetOps(val self: Offset) extends AnyVal {

    def inc[F[_]: ApplicativeThrowable]: F[Offset] = Offset.of[F](self.value + 1)

    def dec[F[_]: ApplicativeThrowable]: F[Offset] = Offset.of[F](self.value - 1)
  }

  implicit val encodeByNamePartition: EncodeByName[Partition] = EncodeByName[Int].contramap { (a: Partition) =>
    a.value
  }

  implicit val decodeByNamePartition: DecodeByName[Partition] = DecodeByName[Int].map(a => Partition.of[Try](a).get)

  implicit val encodeByNameOffset: EncodeByName[Offset] = EncodeByName[Long].contramap((a: Offset) => a.value)

  implicit val decodeByNameOffset: DecodeByName[Offset] = DecodeByName[Long].map(a => Offset.of[Try](a).get)

  implicit val decodeRowPartition: DecodeRow[Partition] = (data: GettableByNameData) =>
    data.decode[Partition]("partition")

  implicit val encodeRowPartition: EncodeRow[Partition] = new EncodeRow[Partition] {

    def apply[B <: SettableData[B]](data: B, partition: Partition) =
      data.encode("partition", partition.value)
  }

  implicit val decodeRowOffset: DecodeRow[Offset] = (data: GettableByNameData) => data.decode[Offset]("offset")

  implicit val encodeRowOffset: EncodeRow[Offset] = new EncodeRow[Offset] {

    def apply[B <: SettableData[B]](data: B, offset: Offset) =
      data.encode("offset", offset.value)
  }

  implicit val hashPartition: Hash[Partition] = Hash.by(_.value)

  implicit class PartitionOps(val self: Partition) extends AnyVal {
    def inc[F[_]: ApplicativeThrow]: F[Partition] = Partition.of[F](self.value + 1)
  }
}
