package com.evolutiongaming.kafka.journal.eventual.cassandra

import com.evolutiongaming.scassandra.TableName

final case class Schema(
  journal: TableName,
  metadata: TableName, // is not used // TODO MR remove with next major release
  metaJournal: TableName,
  pointer: TableName, // TODO MR remove with next major release
  pointer2: TableName,
  setting: TableName,
)
