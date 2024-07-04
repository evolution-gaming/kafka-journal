package com.evolutiongaming.kafka.journal

import java.time.Instant

/** Snapshot to be written, or loaded from a storage, together with a metadata.
  *
  * Technically, if there is no Akka cluster brain split etc., there still could exists two [[SnapshotRecord]] instances
  * per single [[Snapshot]] value. I.e. if one cluster node requests a snapshot to be persisted and then goes down, and
  * a second node recovers before a snapshot propagates to all Cassandra nodes, and then decides to write a snapshot too.
  *
  * @param snapshot
  *   Snapshot and sequence number.
  * @param timestamp
  *   Time when the record was created. I.e. when [[Snapshot]] came in into Kafka Journal and was wrapped into
  *   [[SnapshotRecord]] before being written to an undelrying storage (i.e. Cassandra). This value could be useful, for
  *   example, to analyze the lag between the last written snapshot and a current time.
  * @param origin
  *   The host, which produced a snapshot. See [[Origin]] for more details.
  * @param version
  *   The version of a library, which produced a snapshot. See [[Version]] for more details.
  */
final case class SnapshotRecord[A](
  snapshot: Snapshot[A],
  timestamp: Instant,
  origin: Option[Origin],
  version: Option[Version],
)
