package com.evolutiongaming.kafka.journal.ally.cassandra

import com.evolutiongaming.kafka.journal.Alias.{Id, Timestamp}
import com.evolutiongaming.skafka.Topic

case class Metadata(
  id: Id,
  topic: Topic, // TODO we'd better have this stored, however not needed for reading
  segmentSize: Int,
  created: Timestamp /*TODO use Instant*/,
  updated: Timestamp)