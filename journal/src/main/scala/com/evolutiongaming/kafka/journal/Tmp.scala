package com.evolutiongaming.kafka.journal

import com.evolutiongaming.kafka.journal.Alias.SeqNr
import com.evolutiongaming.kafka.journal.EventsSerializer.EventsFromBytes
import com.evolutiongaming.skafka.consumer.ConsumerRecord
import com.evolutiongaming.skafka.{Bytes, Topic}

// TODO rename
object Tmp {

  case class Result(deleteTo: SeqNr, entries: Vector[Entry])
  

  def apply(
    result: Result,
    action: Action.AppendOrTruncate,
    record: ConsumerRecord[String, Bytes],
    topic: Topic,
    range: SeqRange) = {

    action match {
      case a: Action.Append =>

        println(s">>>>>> $a")

        val bytes = record.value

        def entries = {
          EventsFromBytes(bytes, topic)
            .events
            .to[Vector]
            .map { event =>
              val tags = Set.empty[String] // TODO
              Entry(event.payload, event.seqNr, tags)
            }
        }

        if (range.contains(a.range)) {
          // TODO we don't need to deserialize entries that are out of scope
          result.copy(entries = result.entries ++ entries)

        } else if (a.range < range) {
          result
        } else if (a.range > range) {
          // TODO stop consuming
          result
        } else {

          val filtered = entries.filter { entry => range contains entry.seqNr }

          if (entries.last.seqNr > range) {
            // TODO stop consuming
            result.copy(entries = result.entries ++ filtered)
          } else {
            result.copy(entries = result.entries ++ filtered)
          }
        }

      case a: Action.Truncate =>
        println(s">>>>>> $a")
        if (a.to > result.deleteTo) {
          val entries = result.entries.dropWhile(_.seqNr <= a.to)
          result.copy(deleteTo = a.to, entries = entries)
        } else {
          result
        }
    }
  }
}
