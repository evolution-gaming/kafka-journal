package com.evolutiongaming.kafka.journal.eventual.cassandra

import cats.Parallel
import cats.data.{NonEmptyList => Nel}
import cats.effect.{Concurrent, Timer}
import cats.syntax.all._
import com.evolutiongaming.catshelper.{BracketThrowable, LogOf}
import com.evolutiongaming.kafka.journal.eventual.cassandra.CassandraHelper._
import com.evolutiongaming.kafka.journal.eventual.cassandra.EventualCassandraConfig.ConsistencyConfig
import com.evolutiongaming.kafka.journal.{Origin, Settings}
import com.evolutiongaming.scassandra.ToCql.implicits._

import scala.util.Try

object SetupSchema { self =>

  def migrate[F[_]: BracketThrowable: CassandraSession](
    schema: Schema,
    fresh: CreateSchema.Fresh,
    settings: Settings[F],
    cassandraSync: CassandraSync[F]
  ): F[Unit] = {

    def addHeaders = {
      JournalStatements
        .addHeaders(schema.journal)
        .execute
        .first
        .void
        .handleError { _ => () }
    }

    def addVersion = {
      JournalStatements
        .addVersion(schema.journal)
        .execute
        .first
        .void
        .handleError { _ => () }
    }

    def dropMetadata = {
      s"DROP TABLE IF EXISTS ${ schema.metadata.toCql }"
        .execute
        .first
        .void
        .handleError { _ => () }
    }

    def createPointer2 = {
      Pointer2Statements.createTable(schema.pointer2)
        .execute
        .first
        .void
        .handleError { _ => () }
    }

    val schemaVersion = "schema-version"

    val migrations = Nel.of(
      addHeaders,
      addVersion,
      dropMetadata,
      createPointer2)

    def setVersion(version: Int) = {
      settings
        .set("schema-version", version.toString)
        .void
    }

    def migrate = {

      def migrate(version: Int) = {
        migrations
          .toList
          .drop(version + 1)
          .toNel
          .map { migrations =>
            migrations
              .foldLeftM(version) { (version, migration) =>
                val version1 = version + 1
                for {
                  _ <- migration
                  _ <- setVersion(version1)
                } yield version1
              }
              .void
          }
      }

      settings
        .get(schemaVersion)
        .map { setting =>
          setting
            .flatMap { a =>
              Try
                .apply { a.value.toInt }
                .toOption
            }
            .fold {
              if (fresh) {
                val version = migrations.size - 1
                if (version >= 0) {
                  setVersion(version).some
                } else {
                  none[F[Unit]]
                }
              } else {
                migrate(-1)
              }
            } { version =>
              migrate(version)
            }
        }
    }

    migrate.flatMap { migrate1 =>
      migrate1.foldMapM { _ =>
        cassandraSync {
          migrate.flatMap { migrate =>
            migrate.foldMapM(identity)
          }
        }
      }
    }
  }

  def apply[F[_]: Concurrent: Parallel: Timer: CassandraCluster: CassandraSession: LogOf](
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
