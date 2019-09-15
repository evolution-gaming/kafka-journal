package com.evolutiongaming.kafka.journal

import cats.Show
import cats.implicits._
import cats.kernel.Order
import com.evolutiongaming.kafka.journal.util.ApplicativeString
import com.evolutiongaming.kafka.journal.util.OptionHelper._
import com.evolutiongaming.kafka.journal.util.PlayJsonHelper._
import com.evolutiongaming.kafka.journal.util.ScodecHelper._
import com.evolutiongaming.kafka.journal.util.TryHelper._
import com.evolutiongaming.scassandra._
import play.api.libs.json._
import scodec.{Attempt, Codec, codecs}

import scala.util.Try

sealed abstract class SeqNr(val value: Long) {

  override def toString: String = value.toString
}

object SeqNr {

  val min: SeqNr = new SeqNr(1L) {}

  val max: SeqNr = new SeqNr(Long.MaxValue) {}


  implicit val showSeqNr: Show[SeqNr] = Show.fromToString


  implicit val orderingSeqNr: Ordering[SeqNr] = (x: SeqNr, y: SeqNr) => x.value compare y.value

  implicit val orderSeqNr: Order[SeqNr] = Order.fromOrdering


  implicit val encodeByNameSeqNr: EncodeByName[SeqNr] = EncodeByName[Long].imap((seqNr: SeqNr) => seqNr.value)

  implicit val decodeByNameSeqNr: DecodeByName[SeqNr] = DecodeByName[Long].map(value => SeqNr.of[Try](value).get)


  implicit val encodeByNameOptSeqNr: EncodeByName[Option[SeqNr]] = EncodeByName.opt[SeqNr]

  implicit val decodeByNameOptSeqNr: DecodeByName[Option[SeqNr]] = DecodeByName[Option[Long]].map { value =>
    for {
      a <- value
      a <- SeqNr.of[Option](a)
    } yield a
  }


  implicit val encodeRowSeqNr: EncodeRow[SeqNr] = EncodeRow[SeqNr]("seq_nr")

  implicit val decodeRowSeqNr: DecodeRow[SeqNr] = DecodeRow[SeqNr]("seq_nr")


  implicit val writesSeqNr: Writes[SeqNr] = Writes.of[Long].contramap(_.value)

  implicit val readsSeqNr: Reads[SeqNr] = Reads.of[Long].mapResult { a => SeqNr.of[JsResult](a) }


  implicit val codecSeqNr: Codec[SeqNr] = {
    val to = (a: Long) => SeqNr.of[Attempt](a)
    val from = (a: SeqNr) => Attempt.successful(a.value)
    codecs.int64.exmap(to, from)
  }


  def of[F[_] : ApplicativeString](value: Long): F[SeqNr] = {
    if (value < min.value) {
      s"invalid SeqNr of $value, it must be greater or equal to $min".raiseError[F, SeqNr]
    } else if (value > max.value) {
      s"invalid SeqNr of $value, it must be less or equal to $max".raiseError[F, SeqNr]
    } else if (value == min.value) {
      min.pure[F]
    } else if (value == max.value) {
      max.pure[F]
    } else {
      new SeqNr(value) {}.pure[F]
    }
  }


  def opt(value: Long): Option[SeqNr] = of[Option](value)


  def unsafe[A](value: A)(implicit numeric: Numeric[A]): SeqNr = of[Try](numeric.toLong(value)).get


  implicit class SeqNrOps(val self: SeqNr) extends AnyVal {

    def next[F[_] : ApplicativeString]: F[SeqNr] = map[F](_ + 1L)

    def prev[F[_] : ApplicativeString]: F[SeqNr] = map[F](_ - 1L)

    def in(range: SeqRange): Boolean = range contains self

    def to(seqNr: SeqNr): SeqRange = SeqRange(self, seqNr)

    def map[F[_] : ApplicativeString](f: Long => Long): F[SeqNr] = SeqNr.of[F](f(self.value))
  }
}