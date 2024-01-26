package com.evolutiongaming.kafka.journal.eventual.cassandra

import cats.Monad
import cats.data.{NonEmptyList => Nel}
import cats.effect.Concurrent
import cats.syntax.all._
import com.evolutiongaming.catshelper.LogOf
import com.evolutiongaming.scassandra.TableName

object CreateSchema {

  def apply[F[_] : Concurrent : CassandraCluster : CassandraSession : CassandraSync : LogOf](
    config: SchemaConfig
  ): F[(Schema, MigrateSchema.Fresh)] = {

    for {
      createTables   <- CreateTables.of[F]
      createKeyspace  = CreateKeyspace[F]
      result         <- apply[F](config, createKeyspace, createTables)
    } yield result
  }

  def apply[F[_] : Monad](
    config: SchemaConfig,
    createKeyspace: CreateKeyspace[F],
    createTables: CreateTables[F]
  ): F[(Schema, MigrateSchema.Fresh)] = {

    def createTables1 = {
      val keyspace = config.keyspace.name

      val schema = Schema(
        journal = TableName(keyspace = keyspace, table = config.journalTable),
        metadata = TableName(keyspace = keyspace, table = config.metadataTable),
        metaJournal = TableName(keyspace = keyspace, table = config.metaJournalTable),
        pointer = TableName(keyspace = keyspace, table = config.pointerTable),
        pointer2 = TableName(keyspace = keyspace, table = config.pointer2Table),
        setting = TableName(keyspace = keyspace, table = config.settingTable)
      )

      val journalStatements = JournalStatements.createTable(schema.journal)
      val metaJournalStatements = MetaJournalStatements.createTable(schema.metaJournal)
      val pointerStatements = PointerStatements.createTable(schema.pointer)
      val pointer2Statements = Pointer2Statements.createTable(schema.pointer2)
      val settingStatements = SettingStatements.createTable(schema.setting)

      val journal = CreateTables.Table(config.journalTable, journalStatements)
      val metaJournal = CreateTables.Table(config.metaJournalTable, metaJournalStatements)
      val pointer = CreateTables.Table(config.pointerTable, pointerStatements)
      val pointer2 = CreateTables.Table(config.pointer2Table, pointer2Statements)
      val setting = CreateTables.Table(config.settingTable, settingStatements)

      val createSchema =
        if (config.autoCreate) {
          createTables(keyspace, Nel.of(journal, pointer, pointer2, setting, metaJournal))
        } else {
          false.pure[F]
        }

      createSchema.map((schema, _))
    }

    for {
      _      <- createKeyspace(config.keyspace)
      result <- createTables1
    } yield result
  }
}