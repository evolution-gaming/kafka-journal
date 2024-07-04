package com.evolutiongaming.kafka.journal.cassandra

import cats.effect.Clock
import cats.syntax.all.*
import cats.{Monad, Parallel}
import com.evolutiongaming.catshelper.ClockHelper.*
import com.evolutiongaming.kafka.journal.cassandra.CassandraConsistencyConfig
import com.evolutiongaming.kafka.journal.eventual.cassandra.CassandraSession
import com.evolutiongaming.kafka.journal.{Origin, Setting, Settings}
import com.evolutiongaming.scassandra.TableName

object SettingsCassandra {

  def apply[F[_]: Monad: Clock](
    statements: Statements[F],
    origin: Option[Origin],
  ): Settings[F] = new Settings[F] {

    def get(key: K) = {
      for {
        setting <- statements.select(key)
      } yield for {
        setting <- setting
      } yield setting
    }

    def set(key: K, value: V) = {
      for {
        timestamp <- Clock[F].instant
        prev      <- statements.select(key)
        setting    = Setting(key = key, value = value, timestamp = timestamp, origin = origin)
        _         <- statements.insert(setting)
      } yield prev
    }

    def setIfEmpty(key: K, value: V) = {
      for {
        timestamp <- Clock[F].instant
        setting    = Setting(key = key, value = value, timestamp = timestamp, origin = origin)
        inserted  <- statements.insertIfEmpty(setting)
        result    <- if (inserted) none[Setting].pure[F] else statements.select(key)
      } yield result
    }

    def remove(key: K) = {
      for {
        prev <- statements.select(key)
        _    <- statements.delete(key)
      } yield prev
    }

    def all = {
      statements.all
    }
  }

  def of[F[_]: Monad: Parallel: Clock: CassandraSession](
    table: TableName,
    origin: Option[Origin],
    consistencyConfig: CassandraConsistencyConfig,
  ): F[Settings[F]] = {
    for {
      statements <- Statements.of[F](table, consistencyConfig)
    } yield {
      apply(statements, origin)
    }
  }

  final case class Statements[F[_]](
    select: SettingStatements.Select[F],
    insert: SettingStatements.Insert[F],
    insertIfEmpty: SettingStatements.InsertIfEmpty[F],
    all: SettingStatements.All[F],
    delete: SettingStatements.Delete[F],
  )

  object Statements {
    def of[F[_]: Monad: Parallel: CassandraSession](
      table: TableName,
      consistencyConfig: CassandraConsistencyConfig,
    ): F[Statements[F]] = {

      val statements = (
        SettingStatements.Select.of[F](table, consistencyConfig.read),
        SettingStatements.Insert.of[F](table, consistencyConfig.write),
        SettingStatements.InsertIfEmpty.of[F](table, consistencyConfig.write),
        SettingStatements.All.of[F](table, consistencyConfig.read),
        SettingStatements.Delete.of[F](table, consistencyConfig.write),
      )
      statements.parMapN(Statements[F])
    }
  }
}
