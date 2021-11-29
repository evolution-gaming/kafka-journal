package com.evolutiongaming.kafka.journal.eventual.cassandra

import cats.effect.Clock
import cats.syntax.all._
import cats.{Monad, Parallel}
import com.evolutiongaming.catshelper.ClockHelper._
import com.evolutiongaming.kafka.journal.eventual.cassandra.EventualCassandraConfig.ConsistencyConfig
import com.evolutiongaming.kafka.journal.{Origin, Setting, Settings}
import com.evolutiongaming.scassandra.TableName

object SettingsCassandra {

  def apply[F[_] : Monad : Clock](
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


  def of[F[_] : Monad : Parallel : Clock : CassandraSession](
    schema: Schema,
    origin: Option[Origin]
  )(implicit consistencyConfig: ConsistencyConfig): F[Settings[F]] = {
    for {
      statements <- Statements.of[F](schema.setting)
    } yield {
      apply(statements, origin)
    }
  }


  final case class Statements[F[_]](
    select: SettingStatements.Select[F],
    insert: SettingStatements.Insert[F],
    insertIfEmpty: SettingStatements.InsertIfEmpty[F],
    all: SettingStatements.All[F],
    delete: SettingStatements.Delete[F])

  object Statements {
    def of[F[_] : Monad : Parallel : CassandraSession](table: TableName)(implicit consistencyConfig: ConsistencyConfig): F[Statements[F]] = {
      implicit val readConfig = consistencyConfig.read
      implicit val writeConfig = consistencyConfig.write

      val statements = (
        SettingStatements.Select.of[F](table),
        SettingStatements.Insert.of[F](table),
        SettingStatements.InsertIfEmpty.of[F](table),
        SettingStatements.All.of[F](table),
        SettingStatements.Delete.of[F](table))
      statements.parMapN(Statements[F])
    }
  }
}
