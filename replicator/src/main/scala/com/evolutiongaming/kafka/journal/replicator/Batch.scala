package com.evolutiongaming.kafka.journal.replicator

import cats.data.{NonEmptyList => Nel}
import cats.implicits._
import com.evolutiongaming.kafka.journal._

sealed abstract class Batch extends Product {
  def partitionOffset: PartitionOffset
}

object Batch {

  def of(records: Nel[ActionRecord[Action]]): List[Batch] = {

    def cut(appends: Appends, delete: Action.Delete) = {
      val append = appends.records.head.action
      append.range.to <= delete.to
    }

    val result = records.foldLeft(List.empty[Batch]) { (bs, record) =>
      val partitionOffset = record.partitionOffset

      def actionRecord[A <: Action](a: A) = record.copy(action = a)

      def origin = {
        bs.foldRight(Option.empty[Origin]) { (b, origin) =>
          origin orElse {
            b match {
              case b: Batch.Delete  => b.origin
              case _: Batch.Appends => None
            }
          }
        }
      }

      bs match {
        case b :: tail => (b, record.action) match {
          case (b: Appends, a: Action.Append) =>
            val records = actionRecord(a) :: b.records
            Appends(partitionOffset, records) :: tail

          case (b: Appends, a: Action.Delete) =>
            if (cut(b, a)) {
              val delete = Delete(partitionOffset, a.to, origin orElse a.origin)
              delete :: Nil
            } else {
              val delete = Delete(partitionOffset, a.to, a.origin)
              delete :: bs
            }

          case (b: Appends, _: Action.Mark) =>
            Appends(partitionOffset, b.records) :: tail

          case (b: Delete, a: Action.Append) =>
            Appends(partitionOffset, Nel.of(actionRecord(a))) :: b :: tail

          case (b: Delete, a: Action.Delete) =>
            if (a.to > b.seqNr) {
              if (tail.collectFirst { case b: Appends => cut(b, a) } getOrElse false) {
                val delete = Delete(partitionOffset, a.to, origin orElse a.origin)
                delete :: Nil
              } else {
                val delete = Delete(partitionOffset, a.to, b.origin orElse a.origin)
                delete :: tail
              }
            } else {
              val delete = b.copy(partitionOffset = partitionOffset, origin = b.origin orElse a.origin)
              delete :: tail
            }

          case (b: Delete, _: Action.Mark) =>
            b.copy(partitionOffset = partitionOffset) :: tail
        }

        case Nil =>
          record.action match {
            case a: Action.Append => Appends(partitionOffset, Nel.of(actionRecord(a))) :: Nil
            case a: Action.Delete => Delete(partitionOffset, a.to, a.origin) :: Nil
            case _: Action.Mark   => Nil
          }
      }
    }

    result.foldLeft(List.empty[Batch]) { (bs, b) =>
      b match {
        case b: Appends => b.copy(records = b.records.reverse) :: bs
        case b: Delete  => b :: bs
      }
    }
  }


  final case class Appends(partitionOffset: PartitionOffset, records: Nel[ActionRecord[Action.Append]]) extends Batch

  final case class Delete(partitionOffset: PartitionOffset, seqNr: SeqNr, origin: Option[Origin]) extends Batch
}
