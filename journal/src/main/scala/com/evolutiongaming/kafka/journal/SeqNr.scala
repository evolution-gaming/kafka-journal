package com.evolutiongaming.kafka.journal

import com.evolutiongaming.cassandra.{Decode, DecodeRow, Encode, EncodeRow}
import com.evolutiongaming.kafka.journal.PlayJsonHelper._
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

  implicit val EncodeImpl: Encode[SeqNr] = Encode[Long].imap((seqNr: SeqNr) => seqNr.value)

  implicit val DecodeImpl: Decode[SeqNr] = Decode[Long].map(value => SeqNr(value))


  implicit val EncodeOptImpl: Encode[Option[SeqNr]] = Encode.opt[SeqNr]

  implicit val DecodeOptImpl: Decode[Option[SeqNr]] = Decode[Option[Long]].map { value =>
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


  // TODO remove?
  object Helper {

    implicit class LongOps(val self: Long) extends AnyVal {
      def toSeqNr: SeqNr = SeqNr(self)
    }

    implicit class IntOps(val self: Int) extends AnyVal {
      def toSeqNr: SeqNr = self.toLong.toSeqNr
    }

    implicit class OptSeqNrOps(val self: Option[SeqNr]) extends AnyVal {

      // TODO not create new Somes
      def max(that: Option[SeqNr]): Option[SeqNr] = {
        PartialFunction.condOpt((self, that)) {
          case (Some(x), Some(y)) => x max y
          case (Some(x), None)    => x
          case (None, Some(x))    => x
        }
      }

      def toLong: Long = self.fold(0l)(_.value)
    }
  }
}