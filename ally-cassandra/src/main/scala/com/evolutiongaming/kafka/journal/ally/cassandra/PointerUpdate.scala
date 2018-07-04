package com.evolutiongaming.kafka.journal.ally.cassandra

import java.time.Instant

import com.evolutiongaming.skafka.{Offset, Partition, Topic}

case class PointerUpdate(
  topic: Topic,
  partition: Partition,
  offset: Offset,
  updated: Instant)

case class PointerInsert(
  topic: Topic,
  partition: Partition,
  offset: Offset,
  created: Instant,
  updated: Instant)

case class PointerKey(topic: Topic, partition: Partition)

case class PointerSelect(topic: Topic, partition: Partition)
