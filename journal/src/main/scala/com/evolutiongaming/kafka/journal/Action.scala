package com.evolutiongaming.kafka.journal

import java.time.Instant

import com.evolutiongaming.skafka.Offset

sealed trait Action {
  def header: Action.Header
  def timestamp: Instant
}

object Action {

  sealed trait User extends Action

  sealed trait System extends Action


  final case class Append(header: Header.Append, timestamp: Instant, events: Bytes) extends User {
    def range: SeqRange = header.range
  }

  object Append {
    def apply(range: SeqRange, timestamp: Instant, events: Bytes): Append = {
      val header = Header.Append(range)
      Append(header, timestamp, events)
    }
  }


  final case class Delete(header: Header.Delete, timestamp: Instant) extends User {
    def to: SeqNr = header.to
  }

  object Delete {
    def apply(to: SeqNr, timestamp: Instant): Delete = {
      val header = Header.Delete(to)
      Delete(header, timestamp)
    }
  }


  final case class Mark(header: Header.Mark, timestamp: Instant) extends System

  object Mark {
    def apply(id: String, timestamp: Instant): Mark = {
      val header = Header.Mark(id)
      Mark(header, timestamp)
    }
  }


  sealed trait Header

  object Header {

    final case class Append(range: SeqRange) extends Header

    final case class Delete(to: SeqNr) extends Header

    final case class Mark(id: String) extends Header
  }
}


final case class KafkaRecord(key: Key, action: Action)

final case class ActionRecord(action: Action, offset: Offset)