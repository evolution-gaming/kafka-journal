package com.evolution.kafka.journal

import cats.kernel.{Eq, Order}
import cats.syntax.all.*
import cats.{Applicative, Id, Show}
import com.evolution.kafka.journal.util.Fail
import com.evolution.kafka.journal.util.Fail.implicits.*
import com.evolution.kafka.journal.util.PlayJsonHelper.*
import com.evolution.kafka.journal.util.ScodecHelper.*
import play.api.libs.json.*
import scodec.*

sealed abstract case class SeqNr(value: Long) {

  override def toString: String = value.toString
}

object SeqNr {

  val min: SeqNr = new SeqNr(1L) {}

  val max: SeqNr = new SeqNr(Long.MaxValue) {}

  implicit val eqSeqNr: Eq[SeqNr] = Eq.fromUniversalEquals

  implicit val showSeqNr: Show[SeqNr] = Show.fromToString

  implicit val orderingSeqNr: Ordering[SeqNr] = (x: SeqNr, y: SeqNr) => x.value compare y.value

  implicit val orderSeqNr: Order[SeqNr] = Order.fromOrdering

  implicit val writesSeqNr: Writes[SeqNr] = Writes.of[Long].contramap(_.value)

  implicit val readsSeqNr: Reads[SeqNr] = Reads.of[Long].mapResult { a => SeqNr.of[JsResult](a) }

  implicit val codecSeqNr: Codec[SeqNr] = {
    val to = (a: Long) => SeqNr.of[Attempt](a)
    val from = (a: SeqNr) => Attempt.successful(a.value)
    codecs.int64.exmap(to, from)
  }

  def of[F[_]: Applicative: Fail](value: Long): F[SeqNr] = {
    if (value < min.value) {
      s"invalid SeqNr of $value, it must be greater or equal to $min".fail[F, SeqNr]
    } else if (value > max.value) {
      s"invalid SeqNr of $value, it must be less or equal to $max".fail[F, SeqNr]
    } else if (value === min.value) {
      min.pure[F]
    } else if (value === max.value) {
      max.pure[F]
    } else {
      new SeqNr(value) {}.pure[F]
    }
  }

  def opt(value: Long): Option[SeqNr] = of[Option](value)

  def unsafe[A](
    value: A,
  )(implicit
    numeric: Numeric[A],
  ): SeqNr = of[Id](numeric.toLong(value))

  implicit class SeqNrOps(val self: SeqNr) extends AnyVal {

    def next[F[_]: Applicative: Fail]: F[SeqNr] = map[F](_ + 1L)

    def prev[F[_]: Applicative: Fail]: F[SeqNr] = map[F](_ - 1L)

    def in(range: SeqRange): Boolean = range contains self

    def to(seqNr: SeqNr): SeqRange = SeqRange(self, seqNr)

    def map[F[_]: Applicative: Fail](f: Long => Long): F[SeqNr] = SeqNr.of[F](f(self.value))

    def toDeleteTo: DeleteTo = DeleteTo(self)
  }
}
