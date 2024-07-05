package com.evolutiongaming.kafka.journal.eventual.cassandra

import cats.Monad
import cats.data.NonEmptyList as Nel
import cats.effect.Concurrent
import cats.syntax.all.*
import com.evolutiongaming.catshelper.LogOf
import com.evolutiongaming.kafka.journal.cassandra.MigrateSchema
import com.evolutiongaming.scassandra.TableName

import scala.annotation.nowarn

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
  @nowarn
  // TODO MR deal with deprecated
  def apply[F[_]: Concurrent: CassandraCluster: CassandraSession: CassandraSync: LogOf](
    config: SchemaConfig,
  ): F[(Schema, MigrateSchema.Fresh)] = {
    implicit val cassandraSync2 = CassandraSync[F].toCassandraSync2
    for {
      createTables  <- CreateTables.of[F]
      createKeyspace = CreateKeyspace[F]
      result        <- apply[F](config, createKeyspace, createTables)
    } yield result
  }

  @nowarn
  // TODO MR deal with deprecated
  def apply[F[_]: Monad](
    config: SchemaConfig,
    createKeyspace: CreateKeyspace[F],
    createTables: CreateTables[F],
  ): F[(Schema, MigrateSchema.Fresh)] = {

    def createTables1 = {
      val keyspace = config.keyspace.name

      val schema = Schema(
        journal     = TableName(keyspace = keyspace, table = config.journalTable),
        metadata    = TableName(keyspace = keyspace, table = config.metadataTable),
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
      _      <- createKeyspace(config.keyspace)
      result <- createTables1
    } yield result
  }
}
