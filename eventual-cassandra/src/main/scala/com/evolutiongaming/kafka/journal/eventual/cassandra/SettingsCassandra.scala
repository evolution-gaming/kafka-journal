package com.evolutiongaming.kafka.journal.eventual.cassandra

import cats.{FlatMap, Monad}
import cats.effect.Clock
import cats.implicits._
import com.evolutiongaming.kafka.journal.ClockHelper._
import com.evolutiongaming.kafka.journal.Setting.{Key, Value}
import com.evolutiongaming.kafka.journal.stream.Stream
import com.evolutiongaming.kafka.journal.{HostName, Par, Setting, Settings}
import com.evolutiongaming.scassandra.TableName

object SettingsCassandra {

  def apply[F[_] : FlatMap : Clock](
    statements: Statements[F],
    hostName: Option[HostName]): Settings[F] = new Settings[F] {

    def get(key: Key) = {
      for {
        setting <- statements.select(key)
      } yield for {
        setting <- setting
      } yield setting
    }

    def set(key: Key, value: Value) = {
      val origin = hostName.map(_.value)
      for {
        timestamp <- Clock[F].instant
        prev      <- statements.select(key)
        setting    = Setting(key = key, value = value, timestamp = timestamp, origin = origin)
        _         <- statements.insert(setting)
      } yield prev
    }

    def remove(key: Value) = {
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
      apply(statements, hostName)
    }
  }


  final case class Statements[F[_]](
    select: SettingStatement.Select[F],
    insert: SettingStatement.Insert[F],
    all: SettingStatement.All[F],
    delete: SettingStatement.Delete[F])

  object Statements {
    def of[F[_] : Monad : Par : CassandraSession](table: TableName): F[Statements[F]] = {
      val statements = (
        SettingStatement.Select.of[F](table),
        SettingStatement.Insert.of[F](table),
        SettingStatement.All.of[F](table),
        SettingStatement.Delete.of[F](table))
      Par[F].mapN(statements)(Statements[F])
    }
  }
}
