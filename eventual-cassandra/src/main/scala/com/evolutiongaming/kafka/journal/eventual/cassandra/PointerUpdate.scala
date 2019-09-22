package com.evolutiongaming.kafka.journal.eventual.cassandra

import java.time.Instant

import com.evolutiongaming.skafka.{Offset, Partition, Topic}

final case class PointerUpdate(
  topic: Topic,
  partition: Partition,
  offset: Offset,
  updated: Instant)

final case class PointerInsert(
  topic: Topic,
  partition: Partition,
  offset: Offset,
  created: Instant,
  updated: Instant)
