package com.evolutiongaming.kafka.journal

import cats.data.{NonEmptyList => Nel}
import cats.implicits._
import com.evolutiongaming.kafka.journal.util.{ApplicativeString, MonadString}
import com.evolutiongaming.kafka.journal.util.OptionHelper._
import com.evolutiongaming.kafka.journal.util.TryHelper._
import play.api.libs.json.{Json, OFormat}

import scala.annotation.tailrec
import scala.util.Try

// TODO refactor the way SeqNr done
final case class SeqRange(from: SeqNr, to: SeqNr) {

  require(from <= to, s"from($from) <= to($to)")

  def <(seqNr: SeqNr): Boolean = to < seqNr

  def >(seqNr: SeqNr): Boolean = from > seqNr

  def <(range: SeqRange): Boolean = this < range.from

  def >(range: SeqRange): Boolean = this > range.to

  def intersects(range: SeqRange): Boolean = {
    !(this > range || this < range)
  }

  def contains(seqNr: SeqNr): Boolean = from <= seqNr && to >= seqNr

  def contains(range: SeqRange): Boolean = from <= range.from && to >= range.to

  def toNel: Nel[SeqNr] = {

    @tailrec
    def loop(xs: Nel[SeqNr]): Nel[SeqNr] = xs.head.prev[Option] match {
      case Some(seqNr) if seqNr >= from => loop(seqNr :: xs)
      case _                            => xs
    }

    loop(Nel.of(to))
  }

  override def toString: String = {
    if (from === to) from.toString
    else s"$from..$to"
  }
}

object SeqRange {

  implicit val formatSeqRange: OFormat[SeqRange] = Json.format

  val all: SeqRange = SeqRange(SeqNr.min, SeqNr.max)


  def apply(value: SeqNr): SeqRange = SeqRange(value, value)


  def of[F[_] : ApplicativeString](value: Long): F[SeqRange] = {
    for {
      seqNr <- SeqNr.of[F](value)
    } yield {
      SeqRange(seqNr)
    }
  }

  def of[F[_] : MonadString](from: Long, to: Long): F[SeqRange] = {
    for {
      from <- SeqNr.of[F](from)
      to   <- SeqNr.of[F](to)
    } yield {
      SeqRange(from, to)
    }
  }
  

  def unsafe[A](value: A)(implicit numeric: Numeric[A]): SeqRange = {
    of[Try](numeric.toLong(value)).get
  }

  def unsafe[A](from: A, to: A)(implicit numeric: Numeric[A]): SeqRange = {
    of[Try](from = numeric.toLong(from), to = numeric.toLong(to)).get
  }
}