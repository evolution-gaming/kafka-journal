package com.evolutiongaming.kafka.journal.replicator

import java.time.Instant

import cats.data.{NonEmptyList => Nel}
import cats.implicits._
import com.evolutiongaming.kafka.journal._
import com.evolutiongaming.skafka.Offset
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import scodec.bits.ByteVector

class BatchSpec extends AnyFunSuite with Matchers {
  import BatchSpec._

  private val keyOf = Key(id = "id", topic = "topic")

  private val timestamp = Instant.now()

  for {
    (values, expected) <- List[(Nel[A], List[Batch])](
      (Nel.of(
        mark(offset = 0)), Nil),

      (Nel.of(
        mark(offset = 0),
        mark(offset = 1)), Nil),

      (Nel.of(
        append(offset = 0, seqNr = 1)),
        List(
          appends(0,
            append(offset = 0, seqNr = 1)))),

      (Nel.of(
        append(offset = 0, seqNr = 1, seqNrs = 2)),
        List(
          appends(0,
            append(offset = 0, seqNr = 1, seqNrs = 2)))),

      (Nel.of(
        append(offset = 0, seqNr = 1, seqNrs = 2),
        append(offset = 1, seqNr = 3, seqNrs = 4)),
        List(
          appends(1,
            append(offset = 0, seqNr = 1, seqNrs = 2),
            append(offset = 1, seqNr = 3, seqNrs = 4)))),

      (Nel.of(
        append(offset = 0, seqNr = 1),
        mark(offset = 1)),
        List(
          appends(1,
            append(offset = 0, seqNr = 1)))),

      (Nel.of(
        mark(offset = 0),
        append(offset = 1, seqNr = 1)),
        List(
          appends(1,
            append(offset = 1, seqNr = 1)))),

      (Nel.of(
        append(offset = 0, seqNr = 1),
        append(offset = 1, seqNr = 2)),
        List(
          appends(1,
            append(offset = 0, seqNr = 1),
            append(offset = 1, seqNr = 2)))),

      (Nel.of(
        mark(offset = 0),
        append(offset = 1, seqNr = 1),
        mark(offset = 2),
        append(offset = 3, seqNr = 2),
        mark(offset = 4)),
        List(
          appends(4,
            append(offset = 1, seqNr = 1),
            append(offset = 3, seqNr = 2)))),

      (Nel.of(
        delete(offset = 1, seqNr = 1)),
        List(
          deletes(offset = 1, seqNr = 1))),

      (Nel.of(
        mark(offset = 1),
        delete(offset = 2, seqNr = 1)),
        List(
          deletes(offset = 2, seqNr = 1))),

      (Nel.of(
        delete(offset = 1, seqNr = 1),
        mark(offset = 2)),
        List(
          deletes(offset = 2, seqNr = 1))),

      (Nel.of(
        delete(offset = 1, seqNr = 1),
        append(offset = 2, seqNr = 2)),
        List(
          deletes(offset = 1, seqNr = 1),
          appends(2,
            append(offset = 2, seqNr = 2)))),

      (Nel.of(
        append(offset = 1, seqNr = 2),
        delete(offset = 2, seqNr = 1)),
        List(
          appends(1,
            append(offset = 1, seqNr = 2)),
          deletes(offset = 2, seqNr = 1))),

      (Nel.of(
        append(offset = 1, seqNr = 1, seqNrs = 2, 3),
        delete(offset = 2, seqNr = 1)),
        List(
          appends(1,
            append(offset = 1, seqNr = 1, seqNrs = 2, 3)),
          deletes(offset = 2, seqNr = 1))),

      (Nel.of(
        append(offset = 1, seqNr = 1),
        delete(offset = 2, seqNr = 1),
        append(offset = 3, seqNr = 2)),
        List(
          deletes(offset = 2, seqNr = 1),
          appends(3,
            append(offset = 3, seqNr = 2)))),

      (Nel.of(
        append(offset = 1, seqNr = 1),
        delete(offset = 2, seqNr = 1, origin = "origin1"),
        append(offset = 3, seqNr = 2),
        delete(offset = 4, seqNr = 2, origin = "origin2")),
        List(
          deletes(offset = 4, seqNr = 2, origin = "origin1"))),

      (Nel.of(
        append(offset = 1, seqNr = 1),
        delete(offset = 2, seqNr = 1, origin = "origin"),
        append(offset = 3, seqNr = 2),
        delete(offset = 4, seqNr = 2)),
        List(
          deletes(offset = 4, seqNr = 2, origin = "origin"))),

      (Nel.of(
        append(offset = 1, seqNr = 1),
        append(offset = 2, seqNr = 2),
        delete(offset = 3, seqNr = 1, origin = "origin1"),
        delete(offset = 4, seqNr = 2, origin = "origin2")),
        List(
          deletes(offset = 4, seqNr = 2, origin = "origin1"))),

      (Nel.of(
        append(offset = 1, seqNr = 1),
        append(offset = 2, seqNr = 2),
        delete(offset = 3, seqNr = 1),
        delete(offset = 4, seqNr = 2, origin = "origin")),
        List(
          deletes(offset = 4, seqNr = 2, origin = "origin"))),

      (Nel.of(
        delete(offset = 2, seqNr = 1),
        delete(offset = 3, seqNr = 2)),
        List(
          deletes(offset = 3, seqNr = 2))),

      (Nel.of(
        delete(offset = 2, seqNr = 2, origin = "origin"),
        delete(offset = 3, seqNr = 1)),
        List(
          deletes(offset = 3, seqNr = 2, origin = "origin"))),

      (Nel.of(
        mark(offset = 2),
        delete(offset = 3, seqNr = 1, origin = "origin"),
        mark(offset = 4),
        delete(offset = 5, seqNr = 2),
        mark(offset = 6)),
        List(
          deletes(offset = 6, seqNr = 2, origin = "origin"))),

      (Nel.of(
        append(offset = 0, seqNr = 1),
        delete(offset = 1, seqNr = 1),
        append(offset = 2, seqNr = 2),
        delete(offset = 3, seqNr = 2),
        append(offset = 4, seqNr = 3)),
        List(
          deletes(offset = 3, seqNr = 2),
          appends(4,
            append(offset = 4, seqNr = 3)))),

      (Nel.of(
        append(offset = 0, seqNr = 1),
        append(offset = 1, seqNr = 2),
        delete(offset = 2, seqNr = 1),
        append(offset = 3, seqNr = 3),
        delete(offset = 4, seqNr = 3),
        append(offset = 5, seqNr = 4)),
        List(
          deletes(offset = 4, seqNr = 3),
          appends(5,
            append(offset = 5, seqNr = 4)))),

      (Nel.of(
        append(offset = 0, seqNr = 1),
        append(offset = 1, seqNr = 2),
        mark(offset = 2),
        delete(offset = 3, seqNr = 1),
        append(offset = 4, seqNr = 3),
        append(offset = 5, seqNr = 4),
        mark(offset = 6)),
        List(
          appends(2,
            append(offset = 0, seqNr = 1),
            append(offset = 1, seqNr = 2)),
          deletes(offset = 3, seqNr = 1),
          appends(6,
            append(offset = 4, seqNr = 3),
            append(offset = 5, seqNr = 4)))),

      (Nel.of(
        append(offset = 0, seqNr = 1),
        append(offset = 1, seqNr = 2),
        append(offset = 2, seqNr = 3),
        delete(offset = 3, seqNr = 1, origin = "origin"),
        append(offset = 4, seqNr = 4),
        append(offset = 5, seqNr = 5),
        delete(offset = 6, seqNr = 2),
        append(offset = 7, seqNr = 6)),
        List(
          appends(2,
            append(offset = 0, seqNr = 1),
            append(offset = 1, seqNr = 2),
            append(offset = 2, seqNr = 3)),
          deletes(offset = 3, seqNr = 1, origin = "origin"),
          appends(5,
            append(offset = 4, seqNr = 4),
            append(offset = 5, seqNr = 5)),
          deletes(offset = 6, seqNr = 2),
          appends(7,
            append(offset = 7, seqNr = 6)))),

      (Nel.of(
        append(offset = 0, seqNr = 1, seqNrs = 2),
        append(offset = 1, seqNr = 3, seqNrs = 4),
        append(offset = 2, seqNr = 5),
        delete(offset = 3, seqNr = 1),
        append(offset = 4, seqNr = 6),
        append(offset = 5, seqNr = 7),
        delete(offset = 6, seqNr = 3),
        append(offset = 7, seqNr = 8)),
        List(
          appends(2,
            append(offset = 0, seqNr = 1, seqNrs = 2),
            append(offset = 1, seqNr = 3, seqNrs = 4),
            append(offset = 2, seqNr = 5)),
          deletes(offset = 3, seqNr = 1),
          appends(5,
            append(offset = 4, seqNr = 6),
            append(offset = 5, seqNr = 7)),
          deletes(offset = 6, seqNr = 3),
          appends(7,
            append(offset = 7, seqNr = 8)))),

      (Nel.of(
        purge(offset = 0)),
        List(purges(offset = 0))),
      
      (Nel.of(
        mark(offset = 0),
        purge(offset = 1)),
        List(purges(offset = 1))),

      (Nel.of(
        purge(offset = 0),
        mark(offset = 1)),
        List(purges(offset = 1))),

      (Nel.of(
        purge(offset = 0, origin = "origin"),
        mark(offset = 1),
        purge(offset = 2)),
        List(purges(offset = 2))),

      (Nel.of(
        purge(offset = 0, origin = "origin0"),
        mark(offset = 1),
        purge(offset = 2, origin = "origin")),
        List(purges(offset = 2, origin = "origin"))),

      (Nel.of(
        append(offset = 0, seqNr = 1),
        purge(offset = 1)),
        List(purges(offset = 1))),

      (Nel.of(
        purge(offset = 0),
        append(offset = 1, seqNr = 1)),
        List(
          purges(offset = 0),
          appends(1,
            append(offset = 1, seqNr = 1)))),

      (Nel.of(
        delete(offset = 0, seqNr = 1),
        purge(offset = 1)),
        List(purges(offset = 1))),

      (Nel.of(
        purge(offset = 0),
        delete(offset = 1, seqNr = 1)),
        List(
          purges(offset = 0),
          deletes(offset = 1, seqNr = 1))))
  } {

    val name = values.toList.mkString(",")
    test(s"of $name") {
      val records = values.map(actionRecordOf)
      val actual = Batch.of(records)
      actual shouldEqual expected
    }
  }

  def appends(offset: Int, a: A.Append, as: A.Append*): Batch.Appends = {
    val partitionOffset = partitionOffsetOf(offset)
    val appends = Nel(a, as.toList).map { a =>
      val action = appendOf(Nel(a.seqNr, a.seqNrs))
      actionRecordOf(action, a.offset)
    }
    Batch.Appends(partitionOffset, appends)
  }

  def deletes(offset: Int, seqNr: Int, origin: String = ""): Batch.Delete = {
    Batch.Delete(
      PartitionOffset(offset = Offset.unsafe(offset)),
      SeqNr.unsafe(seqNr).toDeleteTo,
      originOf(origin))
  }

  def purges(offset: Int, origin: String = ""): Batch.Purge = {
    Batch.Purge(
      PartitionOffset(offset = Offset.unsafe(offset)),
      originOf(origin))
  }

  def append(offset: Int, seqNr: Int, seqNrs: Int*): A.Append = {
    A.Append(offset = offset, seqNr = seqNr, seqNrs = seqNrs.toList)
  }

  def delete(offset: Int, seqNr: Int, origin: String = ""): A = {
    A.Delete(offset = offset, seqNr = seqNr, origin = origin)
  }

  def mark(offset: Int): A = {
    A.Mark(offset = offset)
  }

  def purge(offset: Int, origin: String = ""): A = {
    A.Purge(offset = offset, origin = origin)
  }

  def seqNrOf(value: Int): SeqNr = SeqNr.unsafe(value)

  def originOf(origin: String): Option[Origin] = {
    if (origin.isEmpty) none else Origin(origin).some
  }

  def appendOf(seqNrs: Nel[Int]): Action.Append = {
    Action.Append(
      key = keyOf,
      timestamp = timestamp,
      header = ActionHeader.Append(
        range = SeqRange(seqNrOf(seqNrs.head), seqNrOf(seqNrs.last)),
        payloadType = PayloadType.Binary,
        origin = None,
        metadata = HeaderMetadata.empty,
        expireAfter = None),
      payload = ByteVector.empty,
      headers = Headers.empty)
  }

  def deleteOf(seqNr: Int, origin: String): Action.Delete = {
    Action.Delete(
      keyOf,
      timestamp,
      seqNrOf(seqNr).toDeleteTo,
      originOf(origin))
  }

  def actionOf(a: A): Action = {
    a match {
      case a: A.Append => appendOf(Nel(a.seqNr, a.seqNrs))
      case a: A.Delete => deleteOf(seqNr = a.seqNr, origin = a.origin)
      case a: A.Purge  => Action.Purge(keyOf, timestamp, origin = originOf(a.origin))
      case _: A.Mark   => Action.Mark(keyOf, timestamp, ActionHeader.Mark("id", None))
    }
  }

  def actionRecordOf(a: A): ActionRecord[Action] = {
    val action = actionOf(a)
    actionRecordOf(action, a.offset)
  }

  def actionRecordOf[T <: Action](action: T, offset: Int): ActionRecord[T] = {
    val partitionOffset = partitionOffsetOf(offset)
    ActionRecord(action, partitionOffset)
  }

  def partitionOffsetOf(offset: Int): PartitionOffset = PartitionOffset(offset = Offset.unsafe(offset))
}

object BatchSpec {

  sealed trait A {
    def offset: Int
  }

  object A {

    final case class Append(offset: Int, seqNr: Int, seqNrs: List[Int]) extends A {
      override def toString: String = {
        val range = seqNrs.lastOption.fold(seqNr.toString) { to => s"$seqNr..$to" }
        s"$productPrefix($offset,$range)"
      }
    }

    final case class Delete(offset: Int, seqNr: Int, origin: String) extends A

    final case class Mark(offset: Int) extends A

    final case class Purge(offset: Int, origin: String) extends A
  }
}