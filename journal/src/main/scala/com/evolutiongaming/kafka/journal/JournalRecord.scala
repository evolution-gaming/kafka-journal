package com.evolutiongaming.kafka.journal

import com.evolutiongaming.kafka.journal.Aliases._
import com.evolutiongaming.nel.Nel

case class JournalRecord(
  id: Id,
  timestamp: Timestamp,
  payload: JournalRecord.Payload)


object JournalRecord {

  sealed trait Payload

  object Payload {
    case class Events(events: Nel[Event]) extends Payload
    case object Recover extends Payload
    case object Delete extends Payload
  }

  case class Event(seqNr: SeqNr, payload: Array[Byte])
}
