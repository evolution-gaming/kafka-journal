package com.evolutiongaming.kafka.journal.cassandra

import cats.effect.Clock
import cats.syntax.all.*
import cats.{Monad, Parallel}
import com.evolutiongaming.catshelper.ClockHelper.*
import com.evolutiongaming.kafka.journal.eventual.cassandra.CassandraSession
import com.evolutiongaming.kafka.journal.{Origin, Setting, Settings}
import com.evolutiongaming.scassandra.TableName
import com.evolutiongaming.sstream.Stream

private[journal] object SettingsCassandra {

  def apply[F[_]: Monad: Clock](
    statements: Statements[F],
    origin: Option[Origin],
  ): Settings[F] = new Settings[F] {

    def get(key: K): F[Option[Setting]] = {
      for {
        setting <- statements.select(key)
      } yield for {
        setting <- setting
      } yield setting
    }

    def set(key: K, value: V): F[Option[Setting]] = {
      for {
        timestamp <- Clock[F].instant
        prev <- statements.select(key)
        setting = Setting(key = key, value = value, timestamp = timestamp, origin = origin)
        _ <- statements.insert(setting)
      } yield prev
    }

    def remove(key: K): F[Option[Setting]] = {
      for {
        prev <- statements.select(key)
        _ <- statements.delete(key)
      } yield prev
    }

    def all: Stream[F, Setting] = {
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
        SettingStatements.All.of[F](table, consistencyConfig.read),
        SettingStatements.Delete.of[F](table, consistencyConfig.write),
      )
      statements.parMapN(Statements[F])
    }
  }
}
