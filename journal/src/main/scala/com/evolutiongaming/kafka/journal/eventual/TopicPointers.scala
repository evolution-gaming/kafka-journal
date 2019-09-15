package com.evolutiongaming.kafka.journal.eventual

import com.evolutiongaming.skafka.{Offset, Partition}

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
  val empty: TopicPointers = TopicPointers()
}