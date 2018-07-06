package com.evolutiongaming.kafka.journal.eventual

import java.time.Instant

import com.evolutiongaming.skafka.{Offset, Partition, TopicPartition}

// TODO rename
sealed trait Pointer1 {

}


// TODO describe via ADT ?
case class UpdatePointers(timestamp: Instant, pointers: Map[TopicPartition, (Offset, Option[Instant])])


// TODO what to do if Offset is not in map ?
case class TopicPointers(pointers: Map[Partition, Offset])

object TopicPointers {
  val Empty: TopicPointers = TopicPointers(Map.empty)
}

case class AllPointers(pointers: Map[TopicPartition, Offset])