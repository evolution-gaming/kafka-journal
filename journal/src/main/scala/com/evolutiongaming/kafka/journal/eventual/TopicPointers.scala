package com.evolutiongaming.kafka.journal.eventual

import java.time.Instant

import com.evolutiongaming.skafka.{Offset, Partition, TopicPartition}

// TODO what to do if Offset is not in map ?
case class TopicPointers(pointers: Map[Partition, Offset]) {

  override def toString: String = {
    val pointersStr = pointers.map { case (k, v) => s"$k:$v" }.mkString(",")
    s"$productPrefix($pointersStr)"
  }
}

object TopicPointers {
  val Empty: TopicPointers = TopicPointers(Map.empty)
}


// TODO describe via ADT ?
case class UpdatePointers(timestamp: Instant, pointers: Map[TopicPartition, (Offset, Option[Instant])])