package com.evolutiongaming.kafka.journal.eventual.cassandra

import cats.Monad
import cats.effect.Clock
import cats.implicits._
import com.evolutiongaming.kafka.journal.ClockHelper._
import com.evolutiongaming.kafka.journal.stream.Stream
import com.evolutiongaming.kafka.journal.{HostName, Par, Setting, Settings}
import com.evolutiongaming.scassandra.TableName

object SettingsCassandra {

  def apply[F[_] : Monad : Clock](
    statements: Statements[F],
    origin: Option[String],
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
      for {
        settings <- Stream.lift(statements.all)
        setting  <- settings
      } yield setting
    }
  }


  def of[F[_] : Monad : Par : Clock : CassandraSession](schema: Schema): F[Settings[F]] = {
    for {
      statements <- Statements.of[F](schema.setting)
    } yield {
      val hostName = HostName()
      val origin = hostName.map(_.value)
      apply(statements, origin)
    }
  }


  final case class Statements[F[_]](
    select: SettingStatement.Select[F],
    insert: SettingStatement.Insert[F],
    insertIfEmpty: SettingStatement.InsertIfEmpty[F],
    all: SettingStatement.All[F],
    delete: SettingStatement.Delete[F])

  object Statements {
    def of[F[_] : Monad : Par : CassandraSession](table: TableName): F[Statements[F]] = {
      val statements = (
        SettingStatement.Select.of[F](table),
        SettingStatement.Insert.of[F](table),
        SettingStatement.InsertIfEmpty.of[F](table),
        SettingStatement.All.of[F](table),
        SettingStatement.Delete.of[F](table))
      Par[F].mapN(statements)(Statements[F])
    }
  }
}
