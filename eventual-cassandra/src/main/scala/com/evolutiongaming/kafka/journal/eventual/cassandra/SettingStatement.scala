package com.evolutiongaming.kafka.journal.eventual.cassandra

import java.time.Instant

import cats.implicits._
import cats.{FlatMap, Monad}
import com.datastax.driver.core.{GettableByNameData, SettableData}
import com.evolutiongaming.kafka.journal.Setting
import com.evolutiongaming.kafka.journal.Setting.{Key, Value}
import com.evolutiongaming.kafka.journal.eventual.cassandra.CassandraHelper._
import com.evolutiongaming.kafka.journal.stream.Stream
import com.evolutiongaming.scassandra.syntax._
import com.evolutiongaming.scassandra.{DecodeRow, EncodeRow, TableName}

object SettingStatement {

  implicit val EncodeSetting: EncodeRow[Setting] = new EncodeRow[Setting] {

    def apply[B <: SettableData[B]](data: B, value: Setting) = {
      data
        .encode("key", value.key)
        .encode("value", value.value)
        .encode("timestamp", value.timestamp)
        .encode("origin", value.origin)
    }
  }

  implicit val DecodeSetting: DecodeRow[Setting] = new DecodeRow[Setting] {

    def apply(data: GettableByNameData) = {
      Setting(
        key = data.decode[Key]("key"),
        value = data.decode[Value]("value"),
        timestamp = data.decode[Instant]("timestamp"),
        origin = data.decode[Option[String]]("origin"))
    }
  }

  def createTable(name: TableName): String = {
    s"""
       |CREATE TABLE IF NOT EXISTS ${ name.toCql } (
       |key text PRIMARY KEY,
       |value text,
       |timestamp timestamp,
       |origin text,
       |metadata text)
       |""".stripMargin
  }


  type Select[F[_]] = Key => F[Option[Setting]]

  object Select {

    def of[F[_] : FlatMap : CassandraSession](name: TableName): F[Select[F]] = {
      val query = s"SELECT value, timestamp, origin FROM ${ name.toCql } WHERE key = ?"
      for {
        prepared <- query.prepare
      } yield {
        key: Key =>
          val bound = prepared
            .bind()
            .encode("key", key)
          for {
            result <- bound.execute
          } yield for {
            row <- result.head
          } yield {
            Setting(
              key = key,
              value = row.decode[Value]("value"),
              timestamp = row.decode[Instant]("timestamp"),
              origin = row.decode[Option[String]]("origin"))
          }
      }
    }
  }


  type All[F[_]] = F[Stream[F, Setting]]

  object All {

    def of[F[_] : Monad : CassandraSession](name: TableName): F[All[F]] = {
      val query = s"SELECT key, value, timestamp, origin FROM ${ name.toCql }"
      for {
        prepared <- query.prepare
      } yield {
        val bound = prepared.bind()
        for {
          result <- bound.execute
        } yield for {
          row <- result.stream
        } yield {
          row.decode[Setting]
        }
      }
    }
  }


  type Insert[F[_]] = Setting => F[Unit]

  object Insert {

    def of[F[_] : FlatMap : CassandraSession](name: TableName): F[Insert[F]] = {
      val query = s"INSERT INTO ${ name.toCql } (key, value, timestamp, origin) VALUES (?, ?, ?, ?)"
      for {
        prepared <- query.prepare
      } yield {
        setting: Setting =>
          val bound = prepared
            .bind()
            .encode(setting)
          bound.execute.void
      }
    }
  }


  type Delete[F[_]] = Key => F[Unit]

  object Delete {

    def of[F[_] : FlatMap : CassandraSession](name: TableName): F[Delete[F]] = {
      val query = s"DELETE FROM ${ name.toCql } WHERE key = ?"
      for {
        prepared <- query.prepare
      } yield {
        key: Key =>
          val bound = prepared
            .bind()
            .encode("key", key)
          bound.execute.void
      }
    }
  }
}