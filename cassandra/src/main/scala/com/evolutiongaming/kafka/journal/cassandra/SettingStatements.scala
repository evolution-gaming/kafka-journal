package com.evolutiongaming.kafka.journal.cassandra

import cats.Monad
import cats.syntax.all.*
import com.datastax.driver.core.{GettableByNameData, SettableData}
import com.evolutiongaming.kafka.journal.Setting.{Key, Value}
import com.evolutiongaming.kafka.journal.cassandra.CassandraConsistencyConfig
import com.evolutiongaming.kafka.journal.eventual.cassandra.CassandraHelper.*
import com.evolutiongaming.kafka.journal.eventual.cassandra.CassandraSession
import com.evolutiongaming.kafka.journal.{Origin, Setting}
import com.evolutiongaming.scassandra.syntax.*
import com.evolutiongaming.scassandra.{DecodeRow, EncodeRow, TableName}
import com.evolutiongaming.sstream.Stream

import java.time.Instant

private[journal] object SettingStatements {

  implicit val encodeRowSetting: EncodeRow[Setting] = new EncodeRow[Setting] {

    def apply[B <: SettableData[B]](data: B, value: Setting) = {
      data
        .encode("key", value.key)
        .encode("value", value.value)
        .encode("timestamp", value.timestamp)
        .encode("origin", value.origin)
    }
  }

  implicit val decodeRowSetting: DecodeRow[Setting] = { (data: GettableByNameData) =>
    {
      Setting(
        key       = data.decode[Key]("key"),
        value     = data.decode[Value]("value"),
        timestamp = data.decode[Instant]("timestamp"),
        origin    = data.decode[Option[Origin]],
      )
    }
  }

  def createTable(name: TableName): String = {
    s"""
       |CREATE TABLE IF NOT EXISTS ${name.toCql} (
       |key text PRIMARY KEY,
       |value TEXT,
       |timestamp TIMESTAMP,
       |origin TEXT,
       |metadata TEXT)
       |""".stripMargin
  }

  trait Select[F[_]] {
    def apply(key: Key): F[Option[Setting]]
  }

  object Select {

    def of[F[_]: Monad: CassandraSession](
      name: TableName,
      consistencyConfig: CassandraConsistencyConfig.Read,
    ): F[Select[F]] = {

      val query = s"SELECT value, timestamp, origin FROM ${name.toCql} WHERE key = ?"
      for {
        prepared <- query.prepare
      } yield { (key: Key) =>
        val bound = prepared
          .bind()
          .encode("key", key)
          .setConsistencyLevel(consistencyConfig.value)
        for {
          row <- bound.first
        } yield for {
          row <- row
        } yield {
          Setting(
            key       = key,
            value     = row.decode[Value]("value"),
            timestamp = row.decode[Instant]("timestamp"),
            origin    = row.decode[Option[Origin]],
          )
        }
      }
    }
  }

  type All[F[_]] = Stream[F, Setting]

  object All {

    def of[F[_]: Monad: CassandraSession](name: TableName, consistencyConfig: CassandraConsistencyConfig.Read): F[All[F]] = {

      val query = s"SELECT key, value, timestamp, origin FROM ${name.toCql}"
      for {
        prepared <- query.prepare
      } yield {
        val bound = prepared.setConsistencyLevel(consistencyConfig.value).bind()
        for {
          row <- bound.execute
        } yield {
          row.decode[Setting]
        }
      }
    }
  }

  trait Insert[F[_]] {
    def apply(setting: Setting): F[Unit]
  }

  object Insert {

    def of[F[_]: Monad: CassandraSession](
      name: TableName,
      consistencyConfig: CassandraConsistencyConfig.Write,
    ): F[Insert[F]] = {

      val query = s"INSERT INTO ${name.toCql} (key, value, timestamp, origin) VALUES (?, ?, ?, ?)"
      for {
        prepared <- query.prepare
      } yield { (setting: Setting) =>
        val bound = prepared
          .bind()
          .encode(setting)
          .setConsistencyLevel(consistencyConfig.value)
        bound.first.void
      }
    }
  }

  trait Delete[F[_]] {
    def apply(key: Key): F[Unit]
  }

  object Delete {

    def of[F[_]: Monad: CassandraSession](
      name: TableName,
      consistencyConfig: CassandraConsistencyConfig.Write,
    ): F[Delete[F]] = {

      val query = s"DELETE FROM ${name.toCql} WHERE key = ?"
      for {
        prepared <- query.prepare
      } yield { (key: Key) =>
        val bound = prepared
          .bind()
          .encode("key", key)
          .setConsistencyLevel(consistencyConfig.value)
        bound.first.void
      }
    }
  }
}
