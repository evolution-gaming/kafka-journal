package com.evolutiongaming.kafka.journal

// TODO test
final case class SeqNr(value: Long) extends Ordered[SeqNr] {

  require(SeqNr.isValid(value), SeqNr.invalid(value))

  /*private */ def +(that: Long): SeqNr = copy(this.value + that)

  /*private */ def +(that: SeqNr): SeqNr = this + that.value


  private def -(that: Long): SeqNr = copy(this.value - that)

  private def -(that: SeqNr): SeqNr = this - that.value


  def max(that: SeqNr): SeqNr = if (this.value > that.value) this else that

  def min(that: SeqNr): SeqNr = if (this.value < that.value) this else that

  def min(that: Option[SeqNr]): SeqNr = that.fold(this)(_ min this)

  // TODO
  def next: SeqNr = this + 1

  // TODO
  def prev: SeqNr = this - 1


  def nextOpt: Option[SeqNr] = if (this == SeqNr.Max) None else Some(this + 1)

  def prevOpt: Option[SeqNr] = if (this == SeqNr.Min) None else Some(this - 1)


  def in(range: SeqRange): Boolean = range contains this


  // TODO
  def __(seqNr: SeqNr): SeqRange = SeqRange(this, seqNr)

  // TODO
  def __ : SeqRange = SeqRange(this)

  // TODO
  def to(seqNr: SeqNr): SeqRange = SeqRange(this, seqNr)

  // TODO
  def to: SeqRange = SeqRange(this)


  def compare(that: SeqNr): Int = this.value compare that.value

  override def toString: String = value.toString
}

object SeqNr {
  val Max: SeqNr = SeqNr(Long.MaxValue)
  val Min: SeqNr = SeqNr(1L)

  def validate[T](value: Long)(onError: String => T, onSeqNr: SeqNr => T): T = {
    if (isValid(value)) onSeqNr(SeqNr(value)) else onError(invalid(value))
  }

  def either(value: Long): Either[String, SeqNr] = validate(value)(Left(_), Right(_))

  def opt(value: Long): Option[SeqNr] = validate(value)(_ => None, Some(_))

  def apply(value: Long, fallback: => SeqNr): SeqNr = validate(value)(_ => fallback, identity)

  private def isValid(value: Long) = value > 0 && value <= Long.MaxValue

  private def invalid(value: Long) = s"invalid SeqNr $value, it must be greater 0"


  object Helper {

    implicit class LongOps(val self: Long) extends AnyVal {
      def toSeqNr: SeqNr = SeqNr(self)
    }

    implicit class IntOps(val self: Int) extends AnyVal {
      def toSeqNr: SeqNr = self.toLong.toSeqNr
    }

    implicit class OptSeqNrOps(val self: Option[SeqNr]) extends AnyVal {

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