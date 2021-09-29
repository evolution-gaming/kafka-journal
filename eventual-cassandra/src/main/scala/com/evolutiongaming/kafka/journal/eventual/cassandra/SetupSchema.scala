package com.evolutiongaming.kafka.journal.eventual.cassandra

import cats.Parallel
import cats.effect.Concurrent
import cats.syntax.all._
import com.evolutiongaming.catshelper.{BracketThrowable, LogOf}
import com.evolutiongaming.kafka.journal.eventual.cassandra.CassandraHelper._
import com.evolutiongaming.kafka.journal.{Origin, Settings}
import com.evolutiongaming.scassandra.TableName

import scala.util.Try
import cats.effect.Temporal

object SetupSchema { self =>

  def migrate[F[_]: BracketThrowable: CassandraSession](
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

  def apply[F[_]: Concurrent: Parallel: Temporal: CassandraCluster: CassandraSession: LogOf](
    config: SchemaConfig,
    origin: Option[Origin]
  ): F[Schema] = {

    def createSchema(implicit cassandraSync: CassandraSync[F]) = CreateSchema(config)

    for {
      cassandraSync   <- CassandraSync.of[F](config, origin)
      ab              <- createSchema(cassandraSync)
      (schema, fresh)  = ab
      settings        <- SettingsCassandra.of[F](schema, origin)
      _               <- migrate(schema, fresh, settings, cassandraSync)
    } yield schema
  }
}
