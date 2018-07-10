package com.evolutiongaming.kafka.journal

import java.time.Instant

import com.evolutiongaming.kafka.journal.Alias.{Bytes, Id, SeqNr}
import com.evolutiongaming.skafka.Topic

sealed trait Action {
  def header: Action.Header
}

object Action {

  sealed trait User extends Action {
    def timestamp: Instant
  }

  sealed trait System extends Action

  final case class Append(header: Header.Append, timestamp: Instant, events: Bytes) extends User

  final case class Delete(header: Header.Delete, timestamp: Instant) extends User

  final case class Mark(header: Header.Mark) extends System


  sealed trait Header

  object Header {
    final case class Append(range: SeqRange) extends Header
    final case class Delete(to: SeqNr /*TODO NOT CONFIRMED*/) extends Header
    final case class Mark(id: String) extends Header
  }
}


final case class IdAndTopic(id: Id, topic: Topic) // TODO use for KafkaRecord


// TODO drawback of using type here
final case class KafkaRecord[A <: Action](id: Id, topic: Topic /*TODO not needed here*/ , action: A)