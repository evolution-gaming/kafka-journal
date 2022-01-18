package com.evolutiongaming.kafka.journal.eventual.cassandra

import cats.effect.kernel.Temporal
import cats.syntax.all._
import cats.{MonadThrow, Parallel}
import com.evolutiongaming.catshelper.LogOf
import com.evolutiongaming.kafka.journal.eventual.cassandra.CassandraHelper._
import com.evolutiongaming.kafka.journal.eventual.cassandra.EventualCassandraConfig.ConsistencyConfig
import com.evolutiongaming.kafka.journal.{Origin, Settings}
import com.evolutiongaming.scassandra.TableName

import scala.util.Try

object SetupSchema { self =>

  def migrate[F[_]: MonadThrow: CassandraSession](
    schema: Schema,
    fresh: CreateSchema.Fresh,
    settings: Settings[F],
    cassandraSync: CassandraSync[F]
  ): F[Unit] = {

    def addHeaders(table: TableName) = {
      JournalStatements
        .addHeaders(table)
        .execute
        .first
        .redeem[Unit](_ => (), _ => ())
    }

    def addVersion(table: TableName) = {
      JournalStatements
        .addVersion(table)
        .execute
        .first
        .redeem[Unit](_ => (), _ => ())
    }

    val schemaVersion = "schema-version"

    def migrate(version: Option[Int]) = {
      version match {
        case None =>
          for {
            _ <- {
              if (fresh) {
                ().pure[F]
              } else {
                cassandraSync {
                  for {
                    _ <- addHeaders(schema.journal)
                    _ <- addVersion(schema.journal)
                  } yield {}
                }
              }
            }
            _ <- settings.setIfEmpty(schemaVersion, "1")
          } yield {}
        case Some(0) =>
          for {
            _ <- cassandraSync { addVersion(schema.journal) }
            _ <- settings.set(schemaVersion, "1")
          } yield {}
        case Some(_) =>
          ().pure[F]
      }
    }

    for {
      setting <- settings.get(schemaVersion)
      version  = for {
        setting <- setting
        version <- Try(setting.value.toInt).toOption
      } yield version
      _       <- migrate(version)
    } yield {}
  }

  def apply[F[_]: Temporal : Parallel : CassandraCluster: CassandraSession: LogOf](
    config: SchemaConfig,
    origin: Option[Origin],
    consistencyConfig: ConsistencyConfig
  ): F[Schema] = {

    def createSchema(implicit cassandraSync: CassandraSync[F]) = CreateSchema(config)

    for {
      cassandraSync   <- CassandraSync.of[F](config, origin)
      ab              <- createSchema(cassandraSync)
      (schema, fresh)  = ab
      settings        <- SettingsCassandra.of[F](schema, origin, consistencyConfig)
      _               <- migrate(schema, fresh, settings, cassandraSync)
    } yield schema
  }
}
