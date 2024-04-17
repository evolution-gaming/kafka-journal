package com.evolutiongaming.kafka.journal.snapshot.cassandra

import cats.Monad
import cats.data.{NonEmptyList => Nel}
import cats.effect.Concurrent
import cats.syntax.all._
import com.evolutiongaming.catshelper.LogOf
import com.evolutiongaming.kafka.journal.cassandra.{CassandraSync, CreateKeyspace, CreateTables, MigrateSchema, SettingStatements}
import com.evolutiongaming.kafka.journal.eventual.cassandra._
import com.evolutiongaming.scassandra.TableName

object CreateSnapshotSchema {

  /** Creates Cassandra schema for storage of a snapshot.
    *
    * The class does not perform a schema migration if any of the tables are
    * already present in a database, and relies on a caller to use a returned
    * value to perfom the necessary migrations afterwards.
    * 
    * @return
    *   Fully qualified table names, and `true` if all of the tables were
    *   created from scratch, or `false` if one or more of them were already
    *   present in a keyspace.
    */
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

      val schema = SnapshotSchema(
        snapshot = TableName(keyspace = keyspace, table = config.snapshotTable),
        setting = TableName(keyspace = keyspace, table = config.settingTable)
      )

      val snapshotStatement = SnapshotStatements.createTable(schema.snapshot)
      val settingStatement = SettingStatements.createTable(schema.setting)

      val snapshot = CreateTables.Table(config.snapshotTable, snapshotStatement)
      val setting = CreateTables.Table(config.settingTable, settingStatement)

      val createSchema =
        if (config.autoCreate) {
          createTables(keyspace, Nel.of(snapshot, setting))
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