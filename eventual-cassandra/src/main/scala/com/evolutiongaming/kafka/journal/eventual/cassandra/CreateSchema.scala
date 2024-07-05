package com.evolutiongaming.kafka.journal.eventual.cassandra

import cats.Monad
import cats.data.NonEmptyList as Nel
import cats.effect.Concurrent
import cats.syntax.all.*
import com.evolutiongaming.catshelper.LogOf
import com.evolutiongaming.kafka.journal.cassandra.{
  CassandraSync,
  CreateKeyspace,
  CreateTables,
  MigrateSchema,
  SettingStatements,
}
import com.evolutiongaming.scassandra.TableName
import com.evolutiongaming.kafka.journal.eventual.cassandra.{
  CassandraSync as LegacyCassandraSync,
  CreateKeyspace as LegacyCreateKeyspace,
}

object CreateSchema {

  /** Creates Cassandra schema for eventual storage of a journal.
    *
    * The class does not perform a schema migration if any of the tables are
    * already present in a database, and relies on a caller to use a returned
    * value to perform the necessary migrations afterwards.
    * 
    * @return
    *   Fully qualified table names, and `true` if all of the tables were
    *   created from scratch, or `false` if one or more of them were already
    *   present in a keyspace.
    */
  @deprecated(since = "3.4.1", message = "Use `apply1`")
  def apply[F[_]: Concurrent: CassandraCluster: CassandraSession: LegacyCassandraSync: LogOf](
    config: SchemaConfig,
  ): F[(Schema, MigrateSchema.Fresh)] = {
    implicit val cassandraSync2: CassandraSync[F] = LegacyCassandraSync[F].toCassandraSync2
    apply1(config)
  }

  /** Creates Cassandra schema for eventual storage of a journal.
   *
   * The class does not perform a schema migration if any of the tables are
   * already present in a database, and relies on a caller to use a returned
   * value to perform the necessary migrations afterwards.
   *
   * @return
   *   Fully qualified table names, and `true` if all of the tables were
   *   created from scratch, or `false` if one or more of them were already
   *   present in a keyspace.
   */
  def apply1[F[_]: Concurrent: CassandraCluster: CassandraSession: CassandraSync: LogOf](
    config: SchemaConfig,
  ): F[(Schema, MigrateSchema.Fresh)] = {
    for {
      createTables  <- CreateTables.of[F]
      keyspaceConfig = config.keyspace.toKeyspaceConfig
      createKeyspace = CreateKeyspace.apply[F](keyspaceConfig)
      result        <- apply1[F](config, createKeyspace, createTables)
    } yield result
  }

  @deprecated(since = "3.4.1", message = "Use `apply1`")
  def apply[F[_]: Monad](
    config: SchemaConfig,
    createKeyspace: LegacyCreateKeyspace[F],
    createTables: CreateTables[F],
  ): F[(Schema, MigrateSchema.Fresh)] = {
    for {
      createKeyspace2 <- LegacyCreateKeyspace(CreateKeyspace[F])(config.keyspace)
    } yield apply1(config, createKeyspace2, createTables)
  }

  def apply1[F[_]: Monad](
    config: SchemaConfig,
    createKeyspace: CreateKeyspace[F],
    createTables: CreateTables[F],
  ): F[(Schema, MigrateSchema.Fresh)] = {

    def createTables1: F[(Schema, Boolean)] = {
      val keyspace = config.keyspace.name

      val schema = Schema(
        journal     = TableName(keyspace = keyspace, table = config.journalTable),
        metadata    = TableName(keyspace = keyspace, table = config.metadataTable), // gets dropped with migration
        metaJournal = TableName(keyspace = keyspace, table = config.metaJournalTable),
        pointer     = TableName(keyspace = keyspace, table = config.pointerTable),
        pointer2    = TableName(keyspace = keyspace, table = config.pointer2Table),
        setting     = TableName(keyspace = keyspace, table = config.settingTable),
      )

      val journalStatement     = JournalStatements.createTable(schema.journal)
      val metaJournalStatement = MetaJournalStatements.createTable(schema.metaJournal)
      val pointerStatement     = PointerStatements.createTable(schema.pointer)
      val pointer2Statement    = Pointer2Statements.createTable(schema.pointer2)
      val settingStatement     = SettingStatements.createTable(schema.setting)

      val journal     = CreateTables.Table(config.journalTable, journalStatement)
      val metaJournal = CreateTables.Table(config.metaJournalTable, metaJournalStatement)
      val pointer     = CreateTables.Table(config.pointerTable, pointerStatement)
      val pointer2    = CreateTables.Table(config.pointer2Table, pointer2Statement)
      val setting     = CreateTables.Table(config.settingTable, settingStatement)

      val createSchema =
        if (config.autoCreate) {
          createTables(keyspace, Nel.of(journal, pointer, pointer2, setting, metaJournal))
        } else {
          false.pure[F]
        }

      createSchema.map((schema, _))
    }

    for {
      _      <- createKeyspace(config.keyspace.toKeyspaceConfig)
      result <- createTables1
    } yield result
  }
}
