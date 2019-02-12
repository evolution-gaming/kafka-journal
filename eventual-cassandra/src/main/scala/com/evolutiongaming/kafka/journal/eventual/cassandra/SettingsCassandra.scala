package com.evolutiongaming.kafka.journal.eventual.cassandra

import cats.FlatMap
import cats.effect.Clock
import cats.implicits._
import com.evolutiongaming.kafka.journal.Setting.{Key, Value}
import com.evolutiongaming.kafka.journal.stream.Stream
import com.evolutiongaming.kafka.journal.{HostName, Setting, Settings}
import com.evolutiongaming.kafka.journal.ClockHelper._

object SettingsCassandra {

  def apply[F[_] : Clock : FlatMap](
    statements: Statements[F],
    hostName: Option[HostName]): Settings[F] = new Settings[F] {

    def get(key: Key) = {
      for {
        setting <- statements.select(key)
      } yield for {
        setting <- setting
      } yield {
        setting
      }
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

  final case class Statements[F[_]](
    select: SettingStatement.Select[F],
    all: SettingStatement.All[F],
    insert: SettingStatement.Insert[F],
    delete: SettingStatement.Delete[F])
}
