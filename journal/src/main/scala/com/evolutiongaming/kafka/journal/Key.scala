package com.evolutiongaming.kafka.journal

import com.evolutiongaming.skafka.Topic

final case class Key(id: Id, topic: Topic) {
  override def toString = s"$topic:$id"
}