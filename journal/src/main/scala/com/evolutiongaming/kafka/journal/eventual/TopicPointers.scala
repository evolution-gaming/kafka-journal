package com.evolutiongaming.kafka.journal.eventual

import java.time.Instant

import com.evolutiongaming.skafka.{Offset, Partition, TopicPartition}

// TODO what to do if Offset is not in map ?
final case class TopicPointers(values: Map[Partition, Offset] = Map.empty) {

  def +(that: TopicPointers): TopicPointers = {
    copy(this.values ++ that.values)
  }

  override def toString: String = {
    val pointersStr = values.map { case (k, v) => s"$k:$v" }.mkString(",")
    s"$productPrefix($pointersStr)"
  }
}

object TopicPointers {
  val Empty: TopicPointers = TopicPointers()
}


// TODO describe via ADT ?
final case class UpdatePointers(timestamp: Instant, pointers: Map[TopicPartition, (Offset, Option[Instant])])