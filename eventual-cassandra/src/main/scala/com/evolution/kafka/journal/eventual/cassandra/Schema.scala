package com.evolution.kafka.journal.eventual.cassandra

import com.evolutiongaming.scassandra.TableName

/**
 * Describes keyspace scheme. See related classes:
 *   - [[SchemaConfig]]
 *   - [[CreateKeyspace]]
 *   - [[CreateTables]]
 *   - [[SetupSchema]]
 *   - [[MigrateSchema]]
 *   - [[SettingsCassandra]]
 *
 * @param journal
 *   stores all journals, see [[JournalStatements]]
 * @param metaJournal
 *   stores information about active aggregates, see [[MetaJournalStatements]]
 * @param pointer2
 *   stores information on how close to tail the replication is, see [[Pointer2Statements]]
 * @param setting
 *   used to track schema migrations, see [[SetupSchema]] and [[MigrateSchema]] for details
 */
private[journal] final case class Schema(
  journal: TableName,
  metaJournal: TableName,
  pointer2: TableName,
  setting: TableName,
) {

  /**
   * It is expected that all tables are created in the same keyspace (see [[CreateSchema]]).
   */
  def keyspace: String =
    metaJournal.keyspace
}
