package com.evolutiongaming.kafka.journal.eventual.cassandra

import com.evolutiongaming.scassandra.TableName

final case class Schema(
  journal: TableName,
  metadata: TableName,
  metaJournal: Option[TableName],
  pointer: TableName,
  setting: TableName)