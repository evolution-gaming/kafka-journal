package com.evolutiongaming.kafka.journal

import com.evolutiongaming.kafka.journal.Alias.Id
import com.evolutiongaming.skafka.Topic

final case class Key(id: Id, topic: Topic) {
  override def toString: Topic = s"$topic:$id"
}