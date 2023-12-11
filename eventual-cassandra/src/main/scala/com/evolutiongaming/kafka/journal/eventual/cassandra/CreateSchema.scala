package com.evolutiongaming.kafka.journal.eventual.cassandra

import cats.Monad
import cats.data.{NonEmptyList => Nel}
import cats.effect.Concurrent
import cats.syntax.all._
import com.evolutiongaming.catshelper.LogOf
import com.evolutiongaming.scassandra.TableName

object CreateSchema {

  type Fresh = Boolean


  def apply[F[_] : Concurrent : CassandraCluster : CassandraSession : CassandraSync : LogOf](
    config: SchemaConfig
  ): F[(Schema, Fresh)] = {

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
  ): F[(Schema, Fresh)] = {

    def createTables1 = {
      val keyspace = config.keyspace.name

      def tableName(table: CreateTables.Table) = TableName(keyspace = keyspace, table = table.name)

      def table(name: String, query: TableName => Nel[String]) = {
        val tableName = TableName(keyspace = keyspace, table = name)
        CreateTables.Table(name = name, queries = query(tableName))
      }

      val journal = table(config.journalTable, a => Nel.of(JournalStatements.createTable(a)))

      val metaJournal = table(config.metaJournalTable, a => MetaJournalStatements.createTable(a))

      val pointer = table(config.pointerTable, a => Nel.of(PointerStatements.createTable(a)))

      val pointer2 = table(config.pointer2Table, a => Nel.of(Pointer2Statements.createTable(a)))

      val setting = table(config.settingTable, a => Nel.of(SettingStatements.createTable(a)))

      val schema = Schema(
        journal = tableName(journal),
        metadata = TableName(keyspace = keyspace, table = config.metadataTable),
        metaJournal = tableName(metaJournal),
        pointer = tableName(pointer),
        pointer2 = tableName(pointer2),
        setting = tableName(setting))

      if (config.autoCreate) {
        for {
          result <- createTables(keyspace, Nel.of(journal, pointer, pointer2, setting, metaJournal))
        } yield {
          (schema, result)
        }
      } else {
        (schema, false).pure[F]
      }
    }

    for {
      _      <- createKeyspace(config.keyspace)
      result <- createTables1
    } yield result
  }
}
