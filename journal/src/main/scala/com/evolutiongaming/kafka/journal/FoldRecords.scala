package com.evolutiongaming.kafka.journal

import com.evolutiongaming.kafka.journal.Alias.SeqNr
import com.evolutiongaming.kafka.journal.eventual.{EventualRecord, PartitionOffset}


object FoldRecords {

  sealed trait Tmp {

  }

  // TODO rename
  object Tmp {
    case object Empty extends Tmp
    case class DeleteToKnown(deletedTo: Option[SeqNr], records: Vector[EventualRecord]) extends Tmp
    case class DeleteToUnknown(deletedTo: SeqNr) extends Tmp
  }


  def apply(result: Tmp, record: KafkaRecord[_ <: Action], partitionOffset: PartitionOffset): Tmp = {

    val id = record.id
    record.action match {
      case action: Action.Append =>
        val events = EventsSerializer.fromBytes(action.events)
        val records = events.to[Vector]/*TODO*/.map { event =>
          // TODO different created and inserted timestamps
          EventualRecord(
            id = id,
            seqNr = event.seqNr,
            timestamp = action.timestamp,
            payload = event.payload,
            tags = event.tags,
            partitionOffset = partitionOffset)
        }

        result match {
          case Tmp.Empty => Tmp.DeleteToKnown(None, records)

          case Tmp.DeleteToKnown(deleteTo, xs) =>
            Tmp.DeleteToKnown(deleteTo, xs ++ records)

          case Tmp.DeleteToUnknown(value) =>

            val range = action.header.range
            if (value < range.from) {
              Tmp.DeleteToKnown(Some(value), records)
            } else if (value >= range.to) {
              Tmp.DeleteToKnown(Some(range.to), Vector.empty)
            } else {
              val result = records.dropWhile { _.seqNr <= value }
              Tmp.DeleteToKnown(Some(value), result)
            }
        }


      case action: Action.Delete =>

        val deleteTo = action.header.to

        result match {
          case Tmp.Empty => Tmp.DeleteToUnknown(deleteTo)
          // TODO use the same logic in Eventual
          case Tmp.DeleteToKnown(value, records) =>

            value match {
              case None =>
                if (records.isEmpty) {
                  ???
                } else {
                  val head = records.head.seqNr
                  if (deleteTo < head) {
                    Tmp.DeleteToKnown(None, records)
                  } else {
                    val result = records.dropWhile { _.seqNr <= deleteTo }
                    val lastSeqNr = result.lastOption.fold(records.last.seqNr) { _.seqNr }
                    Tmp.DeleteToKnown(Some(lastSeqNr), result)
                  }
                }


              case Some(deleteToPrev) =>
                if (records.isEmpty) {
                  Tmp.DeleteToKnown(Some(deleteToPrev), records)
                } else {
                  if (deleteTo <= deleteToPrev) {
                    Tmp.DeleteToKnown(Some(deleteToPrev), records)
                  } else {
                    val result = records.dropWhile { _.seqNr <= deleteTo }
                    val lastSeqNr = result.lastOption.fold(records.last.seqNr) { _ => deleteTo }
                    Tmp.DeleteToKnown(Some(lastSeqNr), result)
                  }
                }
            }

          case Tmp.DeleteToUnknown(deleteToPrev) => Tmp.DeleteToUnknown(deleteTo max deleteToPrev)
        }

      case action: Action.Mark => result
    }
  }
}


