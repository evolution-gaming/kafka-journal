package com.evolutiongaming.kafka.journal.eventual.cassandra

import cats.data.{NonEmptyList => Nel}
import cats.effect.kernel.Temporal
import cats.syntax.all._
import cats.{MonadThrow, Parallel}
import com.evolutiongaming.catshelper.LogOf
import com.evolutiongaming.kafka.journal.eventual.cassandra.CassandraHelper._
import com.evolutiongaming.kafka.journal.eventual.cassandra.EventualCassandraConfig.ConsistencyConfig
import com.evolutiongaming.kafka.journal.{Origin, Settings}
import com.evolutiongaming.scassandra.ToCql.implicits._

object SetupJournalSchema {

  val SettingKey = "schema-version"

  def migrations[F[_]: MonadThrow: CassandraSession](schema: Schema): Nel[F[Unit]] = {

    def addHeaders =
      JournalStatements
        .addHeaders(schema.journal)
        .execute
        .first
        .void
        .voidError

    def addVersion =
      JournalStatements
        .addVersion(schema.journal)
        .execute
        .first
        .void
        .voidError

    def dropMetadata =
      s"DROP TABLE IF EXISTS ${schema.metadata.toCql}".execute.first.void.voidError

    def createPointer2 =
      Pointer2Statements
        .createTable(schema.pointer2)
        .execute
        .first
        .void
        .voidError

    Nel.of(addHeaders, addVersion, dropMetadata, createPointer2)

  }

  private[eventual] def apply[F[_]: MonadThrow: CassandraSession](
    schema: Schema,
    fresh: CreateSchema.Fresh,
    settings: Settings[F],
    cassandraSync: CassandraSync[F]
  ): F[Unit] = {
    val setupSchema = SetupSchema[F](cassandraSync, settings, SettingKey, migrations(schema).toList)
    setupSchema.migrate(fresh)
  }

  def apply[F[_]: Temporal: Parallel: CassandraCluster: CassandraSession: LogOf](
    config: SchemaConfig,
    origin: Option[Origin],
    consistencyConfig: ConsistencyConfig
  ): F[Schema] = {

    def createSchema(implicit cassandraSync: CassandraSync[F]) = CreateSchema(config)

    for {
      cassandraSync <- CassandraSync.of[F](config.keyspace, config.locksTable, origin)
      ab <- createSchema(cassandraSync)
      (schema, fresh) = ab
      settings <- SettingsCassandra.of[F](schema.setting, origin, consistencyConfig)
      _ <- SetupJournalSchema(schema, fresh, settings, cassandraSync)
    } yield schema
  }

}
