package com.evolutiongaming.kafka.journal.eventual.cassandra

import cats.effect.{Clock, Concurrent, Sync}
import cats.implicits._
import cats.temp.par.Par
import com.evolutiongaming.kafka.journal.{LogOf, Setting, Settings}
import com.evolutiongaming.scassandra.TableName
import com.evolutiongaming.catshelper.EffectHelper._
import com.evolutiongaming.catshelper.{FromFuture, ToFuture}
import com.evolutiongaming.kafka.journal.eventual.cassandra.CassandraHelper._

import scala.util.Try

object SetupSchema { self =>

  def migrate[F[_] : Sync : CassandraSession : CassandraSync : Settings](
    schema: Schema,
    fresh: CreateSchema.Fresh
  ): F[Unit] = {

    def addHeaders(table: TableName)(implicit cassandraSync: CassandraSync[F]) = {
      val query = JournalStatement.addHeaders(table)
      val fa = query.execute.first.redeem[Unit, Throwable](_ => (), _ => ())
      cassandraSync { fa }
    }

    val schemaVersion = "schema-version"

    def version(setting: Option[Setting]) = {
      for {
        setting <- setting
        version <- Try(setting.value.toInt).toOption
      } yield version
    }

    def migrate = {
      for {
        _ <- if (fresh) ().pure[F] else addHeaders(schema.journal)
        _ <- Settings[F].setIfEmpty(schemaVersion, "0")
      } yield {}
    }

    for {
      setting <- Settings[F].get(schemaVersion)
      _       <- version(setting).fold(migrate)(_ => ().pure[F])
    } yield {}
  }

  def apply[F[_] : Concurrent : Par : Clock : CassandraCluster : CassandraSession : FromFuture : ToFuture : LogOf](
    config: SchemaConfig,
  ): F[Schema] = {

    def migrate(
      schema: Schema,
      fresh: CreateSchema.Fresh)(implicit
      cassandraSync: CassandraSync[F],
      settings: Settings[F]
    ) = {

      self.migrate[F](schema, fresh)
    }

    def createSchema(implicit cassandraSync: CassandraSync[F]) = CreateSchema(config)
    
    for {
      cassandraSync   <- CassandraSync.of[F](config)
      ab              <- createSchema(cassandraSync)
      (schema, fresh)  = ab
      settings        <- SettingsCassandra.of[F](schema)
      _               <- migrate(schema, fresh)(cassandraSync, settings)
    } yield schema
  }
}
