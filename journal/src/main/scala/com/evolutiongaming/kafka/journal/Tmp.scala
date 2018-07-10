package com.evolutiongaming.kafka.journal

import com.evolutiongaming.kafka.journal.Alias.SeqNr
import com.evolutiongaming.skafka.Topic

import scala.collection.immutable.Seq

// TODO rename
object Tmp {

  case class Result(deleteTo: SeqNr, events: Seq[Event])


  def apply(
    result: Result,
    action: Action.User,
    topic: Topic,
    range: SeqRange) = {

    action match {
      case action: Action.Append =>

        val bytes = action.events
        val header = action.header

        def events = EventsSerializer.fromBytes(bytes).to[Vector]

        if (range.contains(header.range)) {
          // TODO we don't need to deserialize entries that are out of scope
          result.copy(events = result.events ++ events)

        } else if (header.range < range) {
          result
        } else if (header.range > range) {
          // TODO stop consuming
          result
        } else {

          val filtered = events.filter { entry => range contains entry.seqNr }

          if (events.last.seqNr > range) {
            // TODO stop consuming
            result.copy(events = result.events ++ filtered)
          } else {
            result.copy(events = result.events ++ filtered)
          }
        }

      case action: Action.Delete =>
        val header = action.header
        val deletedTo = header.to
        if (header.to > result.deleteTo) {
          val entries = result.events.dropWhile(_.seqNr <= deletedTo)
          result.copy(deleteTo = deletedTo, events = entries)
        } else {
          result
        }
    }
  }
}
