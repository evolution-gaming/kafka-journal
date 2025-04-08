package com.evolutiongaming.kafka.journal.cassandra

import cats.arrow.FunctionK
import cats.data.State
import cats.effect.Clock
import cats.syntax.all.*
import com.evolutiongaming.catshelper.ClockHelper.*
import com.evolutiongaming.catshelper.{Log, MeasureDuration}
import com.evolutiongaming.kafka.journal.Setting.Key
import com.evolutiongaming.kafka.journal.util.StreamHelper.*
import com.evolutiongaming.kafka.journal.{Origin, Setting}
import com.evolutiongaming.sstream.Stream
import org.scalatest.funsuite.AnyFunSuite

import java.time.Instant
import java.time.temporal.ChronoUnit

class SettingsCassandraSpec extends AnyFunSuite {

  type F[A] = State[Database, A]

  test("set") {
    val program = settings.set(setting.key, setting.value)
    val (database, previousValue) = program.run(Database.empty).value

    assert(database.settings == Map(setting.key -> setting))
    assert(previousValue.isEmpty)
  }

  test("get") {
    val program = settings.get(setting.key)
    val (database, value) = program.run(Database.empty).value
    assert(database.settings.isEmpty)
    assert(value.isEmpty)
  }

  test("all") {
    val program = settings.all.toList
    val (database, values) = program.run(Database.fromSetting(setting)).value
    assert(database.settings == Map(setting.key -> setting))
    assert(values == List(setting))
  }

  test("remove") {
    val program = settings.remove(setting.key)
    val (database, deletedValue) = program.run(Database.fromSetting(setting)).value
    assert(database.settings.isEmpty)
    assert(deletedValue.contains(setting))
  }

  test("set, get, all, remove") {
    val program = for {
      a <- settings.get(setting.key)
      _ = assert(a.isEmpty)
      a <- settings.all.toList
      _ = assert(a.isEmpty)
      a <- settings.remove(setting.key)
      _ = assert(a.isEmpty)

      a <- settings.set(setting.key, setting.value)
      _ = assert(a.isEmpty)
      a <- settings.get(setting.key)
      _ = assert(a.contains(setting))
      a <- settings.all.toList
      _ = assert(a == List(setting))

      a <- settings.remove(setting.key)
      _ = assert(a.contains(setting))
      a <- settings.get(setting.key)
      _ = assert(a.isEmpty)
      a <- settings.all.toList
      _ = assert(a.isEmpty)
      a <- settings.remove(setting.key)
      _ = assert(a.isEmpty)
    } yield ()

    val database = program.runS(Database.empty).value
    assert(database.settings.isEmpty)
  }

  private val origin = Origin("origin")

  private val timestamp = Instant.parse("2023-12-11T11:12:13.00Z").truncatedTo(ChronoUnit.MILLIS)

  private val setting = Setting(key = "key", value = "value", timestamp = timestamp, origin = origin.some)

  private val settings = {

    val statements = SettingsCassandra.Statements(
      select = Database.select(_),
      insert = Database.insert(_),
      all = Database.all,
      delete = Database.delete(_),
    )

    implicit val clock: Clock[F] = Clock.const[F](nanos = 0, millis = timestamp.toEpochMilli)

    implicit val measureDuration: MeasureDuration[F] = MeasureDuration.fromClock(clock)

    SettingsCassandra[F](statements, origin.some)
      .withLog(Log.empty)
      .mapK(FunctionK.id[F], FunctionK.id[F])
  }

  case class Database(settings: Map[Key, Setting]) {

    def exists(key: Key): Boolean =
      select(key).nonEmpty

    def select(key: Key): Option[Setting] =
      settings.get(key)

    def insert(setting: Setting): Database =
      this.copy(settings = settings.updated(setting.key, setting))

    def delete(key: Key): Database =
      this.copy(settings = settings - key)

    def values: List[Setting] =
      settings.values.toList

  }

  object Database {

    val empty: Database = Database(Map.empty)

    def fromSetting(setting: Setting): Database =
      Database(Map(setting.key -> setting))

    def select(key: Key): F[Option[Setting]] =
      State.inspect(_.select(key))

    def all: Stream[F, Setting] =
      values.map(Stream[F].apply(_)).toStream.flatten

    def insert(setting: Setting): F[Unit] =
      State.modify(_.insert(setting))

    def insertIfEmpty(setting: Setting): F[Boolean] =
      exists(setting.key).flatMap { exists =>
        val execute = if (exists) ().pure[F] else insert(setting)
        execute.as(!exists)
      }

    def delete(key: Key): F[Unit] =
      State.modify(_.delete(key))

    def exists(key: Key): F[Boolean] =
      State.inspect(_.exists(key))

    def values: F[List[Setting]] =
      State.inspect(_.values)

  }

}
