package com.evolutiongaming.kafka.journal.eventual

import cats.implicits._
import com.evolutiongaming.skafka.{Offset, Partition}

final case class TopicPointers(values: Map[Partition, Offset] = Map.empty) {

  override def toString: String = {
    val pointersStr = values.map { case (k, v) => s"$k:$v" }.mkString(",")
    s"$productPrefix($pointersStr)"
  }
}

object TopicPointers {

  val empty: TopicPointers = TopicPointers()


  implicit class TopicPointersOps(val self: TopicPointers) extends AnyVal {

    def +(pointers: TopicPointers): TopicPointers = {
      self.copy(self.values ++ pointers.values)
    }

    def merge(pointers: TopicPointers): TopicPointers = {
      val result = for {
        key    <- pointers.values.keySet ++ self.values.keySet
        value0  = pointers.values.get(key)
        value1  = self.values.get(key)
        value  <- (value0 max value1).toList
      } yield {
        (key, value)
      }
      TopicPointers(result.toMap)
    }
  }
}