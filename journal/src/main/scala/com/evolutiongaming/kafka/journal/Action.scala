package com.evolutiongaming.kafka.journal

import java.time.Instant

import com.evolutiongaming.kafka.journal.Alias.{Id, SeqNr}
import com.evolutiongaming.kafka.journal.eventual.PartitionOffset
import com.evolutiongaming.skafka.Topic

sealed trait Action {
  def header: Action.Header
}

object Action {

  sealed trait User extends Action {
    def timestamp: Instant
  }

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


  final case class Mark(header: Header.Mark) extends System

  object Mark {
    def apply(id: String): Mark = {
      val header = Header.Mark(id)
      Mark(header)
    }
  }


  sealed trait Header

  object Header {

    final case class Append(range: SeqRange) extends Header

    final case class Delete(to: SeqNr /*TODO NOT CONFIRMED*/) extends Header {

      require(to > 0, s"to($to) > 0")
    }

    final case class Mark(id: String) extends Header
  }
}


final case class IdAndTopic(id: Id, topic: Topic) // TODO use for KafkaRecord


// TODO drawback of using type here
// TODO do we need this type complexity in general?
final case class KafkaRecord[A <: Action](id: Id, topic: Topic /*TODO not needed here*/ , action: A)

object KafkaRecord {
  type Any = KafkaRecord[_ <: Action]
}

// TODO
final case class KafkaRecord2[A <: Action](record: KafkaRecord[A], partitionOffset: PartitionOffset)