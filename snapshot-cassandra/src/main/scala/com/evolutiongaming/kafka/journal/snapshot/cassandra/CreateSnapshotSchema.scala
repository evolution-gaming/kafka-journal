package com.evolutiongaming.kafka.journal.snapshot.cassandra

import cats.Monad
import cats.data.{NonEmptyList => Nel}
import cats.effect.Concurrent
import cats.syntax.all._
import com.evolutiongaming.catshelper.LogOf
import com.evolutiongaming.kafka.journal.eventual.cassandra._
import com.evolutiongaming.scassandra.TableName

object CreateSnapshotSchema {

  def apply[F[_] : Concurrent : CassandraCluster : CassandraSession : CassandraSync : LogOf](
  config: SnapshotSchemaConfig
  ): F[(SnapshotSchema, MigrateSchema.Fresh)] = {

    for {
      createTables   <- CreateTables.of[F]
      createKeyspace  = CreateKeyspace[F]
      result         <- apply[F](config, createKeyspace, createTables)
    } yield result
  }

  def apply[F[_] : Monad](
    config: SnapshotSchemaConfig,
    createKeyspace: CreateKeyspace[F],
    createTables: CreateTables[F]
  ): F[(SnapshotSchema, MigrateSchema.Fresh)] = {

    def createTables1 = {
      val keyspace = config.keyspace.name

      def tableName(table: CreateTables.Table) = TableName(keyspace = keyspace, table = table.name)

      def table(name: String, query: TableName => Nel[String]) = {
        val tableName = TableName(keyspace = keyspace, table = name)
        CreateTables.Table(name = name, queries = query(tableName))
      }

      val snapshot = table(config.snapshotTable, a => Nel.of(SnapshotStatements.createTable(a)))

      val setting = table(config.settingTable, a => Nel.of(SettingStatements.createTable(a)))

      val schema = SnapshotSchema(
        snapshot = tableName(snapshot),
        setting = tableName(setting))

      if (config.autoCreate) {
        for {
          result <- createTables(keyspace, Nel.of(snapshot, setting))
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
