package com.evolutiongaming.kafka.journal.eventual.cassandra

import cats.data.{NonEmptyList => Nel}
import cats.effect.kernel.Temporal
import cats.syntax.all._
import cats.{MonadThrow, Parallel}
import com.evolutiongaming.catshelper.LogOf
import com.evolutiongaming.kafka.journal.{Origin, Settings}
import com.evolutiongaming.scassandra.ToCql.implicits._

/** Creates a new schema, or migrates to the latest schema version, if it already exists */
object SetupSchema {

  val SettingKey = "schema-version"

  def migrations(schema: Schema): Nel[String] = {

    def addHeaders =
      JournalStatements.addHeaders(schema.journal)

    def addVersion =
      JournalStatements.addVersion(schema.journal)

    def dropMetadata =
      s"DROP TABLE IF EXISTS ${schema.metadata.toCql}"

    def createPointer2 =
      Pointer2Statements.createTable(schema.pointer2)

    Nel.of(addHeaders, addVersion, dropMetadata, createPointer2)

  }

  def migrate[F[_]: MonadThrow: CassandraSession](
    schema: Schema,
    fresh: MigrateSchema.Fresh,
    settings: Settings[F],
    cassandraSync: CassandraSync[F]
  ): F[Unit] = {
    val migrateSchema = MigrateSchema.forSettingKey(
      cassandraSync = cassandraSync.toCassandraSync2,
      settings = settings,
      settingKey = SettingKey,
      migrations = migrations(schema)
    )
    migrateSchema.run(fresh)
  }

  def apply[F[_]: Temporal: Parallel: CassandraCluster: CassandraSession: LogOf](
    config: SchemaConfig,
    origin: Option[Origin],
    consistencyConfig: EventualCassandraConfig.ConsistencyConfig
  ): F[Schema] = {

    def createSchema(implicit cassandraSync: CassandraSync[F]) = CreateSchema(config)

    for {
      cassandraSync <- CassandraSync.of[F](config, origin)
      ab <- createSchema(cassandraSync)
      (schema, fresh) = ab
      settings <- SettingsCassandra.of[F](schema.setting, origin, consistencyConfig.toCassandraConsistencyConfig)
      _ <- migrate(schema, fresh, settings, cassandraSync)
    } yield schema
  }

}
