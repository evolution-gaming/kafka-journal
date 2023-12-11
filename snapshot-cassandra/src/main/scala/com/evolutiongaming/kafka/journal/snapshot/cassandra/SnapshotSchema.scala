package com.evolutiongaming.kafka.journal.snapshot.cassandra

import com.evolutiongaming.scassandra.TableName

final case class SnapshotSchema(snapshot: TableName, setting: TableName)
