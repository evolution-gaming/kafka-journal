package com.evolutiongaming.kafka.journal.eventual.cassandra

import cats.effect.Concurrent
import cats.implicits._
import com.evolutiongaming.kafka.journal.LogOf
import com.evolutiongaming.kafka.journal.util.{FromFuture, ToFuture}
import com.evolutiongaming.nel.Nel
import com.evolutiongaming.scassandra.TableName

// TODO test
object CreateSchema {

  def apply[F[_] : Concurrent : CassandraCluster : CassandraSession : FromFuture : ToFuture : LogOf](
    config: SchemaConfig
  ): F[Schema] = {

    def createTables(implicit cassandraSync: CassandraSync[F]) = {
      val keyspace = config.keyspace.name

      def tableName(table: CreateTables.Table) = TableName(keyspace = keyspace, table = table.name)

      def table(name: String, query: TableName => String) = {
        val tableName = TableName(keyspace = keyspace, table = name)
        CreateTables.Table(name = name, query = query(tableName))
      }

      val journal = table(config.journalTable, JournalStatement.createTable)

      val head = table(config.headTable, HeadStatement.createTable)

      val pointer = table(config.pointerTable, PointerStatement.createTable)

      val setting = table(config.settingTable, SettingStatement.createTable)

      val schema = Schema(
        journal = tableName(journal),
        head = tableName(head),
        pointer = tableName(pointer),
        setting = tableName(setting))

      if (config.autoCreate) {
        for {
          createTables <- CreateTables.of[F]
          _            <- createTables(keyspace, Nel(journal, head, pointer, setting))
        } yield schema
      } else {
        schema.pure[F]
      }
    }

    for {
      _             <- CreateKeyspace[F](config.keyspace)
      cassandraSync <- CassandraSync.of[F](config)
      schema        <- createTables(cassandraSync)
    } yield schema
  }
}