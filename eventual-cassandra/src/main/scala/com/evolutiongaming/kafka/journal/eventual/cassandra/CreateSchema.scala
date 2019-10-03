package com.evolutiongaming.kafka.journal.eventual.cassandra

import cats.Monad
import cats.data.{NonEmptyList => Nel}
import cats.effect.Concurrent
import cats.implicits._
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

      def table(name: String, query: TableName => String) = {
        val tableName = TableName(keyspace = keyspace, table = name)
        CreateTables.Table(name = name, query = query(tableName))
      }

      val journal = table(config.journalTable, JournalStatement.createTable)

      val metadata = table(config.metadataTable, MetadataStatement.createTable)

      val head = table(config.headTable, HeadStatement.createTable)

      val pointer = table(config.pointerTable, PointerStatement.createTable)

      val setting = table(config.settingTable, SettingStatement.createTable)

      val schema = Schema(
        journal = tableName(journal),
        metadata = tableName(metadata),
        head = tableName(head),
        pointer = tableName(pointer),
        setting = tableName(setting))

      if (config.autoCreate) {
        for {
          result <- createTables(keyspace, Nel.of(journal, metadata, pointer, setting))
//          result <- createTables(keyspace, Nel.of(journal, metadata, pointer, head, setting))
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