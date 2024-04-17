package com.evolutiongaming.kafka.journal.eventual.cassandra

import cats.Monad
import cats.data.{NonEmptyList => Nel}
import cats.effect.Concurrent
import cats.syntax.all._
import com.evolutiongaming.catshelper.LogOf
import com.evolutiongaming.kafka.journal.cassandra.MigrateSchema
import com.evolutiongaming.scassandra.TableName

object CreateSchema {

  /** Creates Cassandra schema for eventual storage of a journal.
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
    config: SchemaConfig
  ): F[(Schema, MigrateSchema.Fresh)] = {
    implicit val cassandraSync2 = CassandraSync[F].toCassandraSync2
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

      def table(name: String, query: TableName => Nel[String]) = {
        val tableName = TableName(keyspace = keyspace, table = name)
        CreateTables.Table(name = name, queries = query(tableName))
      }

      val journal = table(config.journalTable, a => Nel.of(JournalStatements.createTable(a)))

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
