package com.evolutiongaming.kafka.journal.eventual.cassandra

import cats.implicits._
import cats.{Order, Show}
import com.evolutiongaming.kafka.journal.util.ApplicativeString
import com.evolutiongaming.kafka.journal.util.TryHelper._
import com.evolutiongaming.scassandra.{DecodeByIdx, DecodeByName, DecodeRow, EncodeByIdx, EncodeByName, EncodeRow}
import pureconfig.error.{CannotParse, ConfigReaderFailures}
import pureconfig.{ConfigCursor, ConfigReader}

import scala.util.Try

sealed abstract case class SegmentSize(value: Int) {
  override def toString: String = value.toString
}

object SegmentSize {

  val min: SegmentSize = new SegmentSize(2) {}

  val max: SegmentSize = new SegmentSize(Int.MaxValue) {}

  val default: SegmentSize = new SegmentSize(100000) {}


  implicit val show: Show[SegmentSize] = Show.fromToString

  implicit val ordering: Ordering[SegmentSize] = Ordering.by(_.value)

  implicit val order: Order[SegmentSize] = Order.fromOrdering


  implicit val encodeByNameSegmentSize: EncodeByName[SegmentSize] = EncodeByName[Int].contramap((a: SegmentSize) => a.value)

  implicit val decodeByNameSegmentSize: DecodeByName[SegmentSize] = DecodeByName[Int].map(a => SegmentSize.of[Try](a).get)


  implicit val encodeByIdxSegmentSize: EncodeByIdx[SegmentSize] = EncodeByIdx[Int].contramap((a: SegmentSize) => a.value)

  implicit val decodeByIdxSegmentSize: DecodeByIdx[SegmentSize] = DecodeByIdx[Int].map(a => SegmentSize.of[Try](a).get)


  implicit val encodeRowSegmentSize: EncodeRow[SegmentSize] = EncodeRow[SegmentSize]("segment_size")

  implicit val decodeRowSegmentSize: DecodeRow[SegmentSize] = DecodeRow[SegmentSize]("segment_size")


  implicit val configReaderSegmentSize: ConfigReader[SegmentSize] = {
    cursor: ConfigCursor => {
      for {
        value       <- cursor.asInt
        segmentSize  = of[Either[String, *]](value)
        segmentSize <- segmentSize.leftMap(a => ConfigReaderFailures(CannotParse(a, cursor.location)))
      } yield segmentSize
    }
  }


  def of[F[_] : ApplicativeString](value: Int): F[SegmentSize] = {
    if (value < min.value) {
      s"invalid SegmentSize of $value, it must be greater or equal to $min".raiseError[F, SegmentSize]
    } else if (value > max.value) {
      s"invalid SegmentSize of $value, it must be less or equal to $max".raiseError[F, SegmentSize]
    } else if (value == min.value) {
      min.pure[F]
    } else if (value == max.value) {
      max.pure[F]
    } else {
      new SegmentSize(value) {}.pure[F]
    }
  }


  def unsafe[A](value: A)(implicit numeric: Numeric[A]): SegmentSize = of[Try](numeric.toInt(value)).get
}
