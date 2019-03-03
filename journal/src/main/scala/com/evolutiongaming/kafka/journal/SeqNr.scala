package com.evolutiongaming.kafka.journal

import cats.kernel.Order
import com.evolutiongaming.kafka.journal.PlayJsonHelper._
import com.evolutiongaming.scassandra._
import play.api.libs.json._

final case class SeqNr(value: Long) extends Ordered[SeqNr] {

  require(SeqNr.isValid(value), SeqNr.invalid(value))

  def max(that: SeqNr): SeqNr = if (this.value > that.value) this else that

  def max(that: Option[SeqNr]): SeqNr = that.fold(this)(_ max this)

  def min(that: SeqNr): SeqNr = if (this.value < that.value) this else that

  def min(that: Option[SeqNr]): SeqNr = that.fold(this)(_ min this)

  def next: Option[SeqNr] = map(_ + 1l)

  def prev: Option[SeqNr] = map(_ - 1l)

  def in(range: SeqRange): Boolean = range contains this

  def to(seqNr: SeqNr): SeqRange = SeqRange(this, seqNr)

  def to: SeqRange = SeqRange(this)

  def compare(that: SeqNr): Int = this.value compare that.value

  override def toString: String = value.toString

  def map(f: Long => Long): Option[SeqNr] = SeqNr.opt(f(value))
}

object SeqNr {
  val Max: SeqNr = SeqNr(Long.MaxValue)
  val Min: SeqNr = SeqNr(1l)

  implicit val EncodeImpl: EncodeByName[SeqNr] = EncodeByName[Long].imap((seqNr: SeqNr) => seqNr.value)

  implicit val DecodeImpl: DecodeByName[SeqNr] = DecodeByName[Long].map(value => SeqNr(value))


  implicit val EncodeOptImpl: EncodeByName[Option[SeqNr]] = EncodeByName.opt[SeqNr]

  implicit val DecodeOptImpl: DecodeByName[Option[SeqNr]] = DecodeByName[Option[Long]].map { value =>
    for {
      value <- value
      seqNr <- SeqNr.opt(value)
    } yield seqNr
  }


  implicit val EncodeRowImpl: EncodeRow[SeqNr] = EncodeRow[SeqNr]("seq_nr")

  implicit val DecodeRowImpl: DecodeRow[SeqNr] = DecodeRow[SeqNr]("seq_nr")


  implicit val WritesImpl: Writes[SeqNr] = WritesOf[Long].imap(_.value)

  implicit val ReadsImpl: Reads[SeqNr] = ReadsOf[Long].mapResult { a =>
    SeqNr.validate(a)(JsError(_), JsSuccess(_))
  }

  implicit val OrderImpl: Order[SeqNr] = new Order[SeqNr] {
    def compare(x: SeqNr, y: SeqNr) = x compare y
  }

  
  def validate[T](value: Long)(onError: String => T, onSeqNr: SeqNr => T): T = {
    if (isValid(value)) onSeqNr(SeqNr(value)) else onError(invalid(value))
  }

  def either(value: Long): Either[String, SeqNr] = validate(value)(Left(_), Right(_))

  val opt: Long => Option[SeqNr] = {
    val onError = (_: String) => None
    validate(_)(onError, Some(_))
  }

  def apply(value: Long, fallback: => SeqNr): SeqNr = validate(value)(_ => fallback, identity)

  private def isValid(value: Long) = value > 0 && value <= Long.MaxValue

  private def invalid(value: Long) = s"invalid SeqNr $value, it must be greater than 0"


  object syntax {

    implicit class SeqNrLongOps(val self: Long) extends AnyVal {
      def toSeqNr: SeqNr = SeqNr(self)
    }

    implicit class SeqNrIntOps(val self: Int) extends AnyVal {
      def toSeqNr: SeqNr = self.toLong.toSeqNr
    }
  }
}