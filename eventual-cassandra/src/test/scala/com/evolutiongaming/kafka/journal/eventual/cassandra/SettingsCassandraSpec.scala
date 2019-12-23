package com.evolutiongaming.kafka.journal.eventual.cassandra

import java.time.Instant
import java.time.temporal.ChronoUnit

import cats.Id
import cats.arrow.FunctionK
import cats.effect.Clock
import cats.implicits._
import com.evolutiongaming.kafka.journal.Setting.Key
import com.evolutiongaming.kafka.journal.{Origin, Setting}
import com.evolutiongaming.catshelper.ClockHelper._
import com.evolutiongaming.catshelper.Log
import com.evolutiongaming.smetrics.MeasureDuration
import com.evolutiongaming.sstream.Stream
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class SettingsCassandraSpec extends AnyFunSuite with Matchers {

  test("set") {
    val (state, prev) = settings.set(setting.key, setting.value).run(State.empty)
    state shouldEqual State(settings = Map((setting.key, setting)))
    prev shouldEqual None
  }

  test("setIfEmpty") {
    val initial = State(settings = Map((setting.key, setting)))
    val (state, current) = settings.setIfEmpty(setting.key, setting.value).run(initial)
    state shouldEqual State(settings = Map((setting.key, setting)))
    current shouldEqual setting.some
  }

  test("get") {
    val (state, value) = settings.get(setting.key).run(State.empty)
    state shouldEqual State.empty
    value shouldEqual None
  }

  test("all") {
    val initial = State(settings = Map((setting.key, setting)))
    val (state, all) = settings.all.toList.run(initial)
    state shouldEqual initial
    all shouldEqual List(setting)
  }

  test("remove") {
    val initial = State(settings = Map((setting.key, setting)))
    val (state, prev) = settings.remove(setting.key).run(initial)
    state shouldEqual State.empty
    prev shouldEqual setting.some
  }

  test("set, get, all, remove") {
    val stateT = for {
      a <- settings.get(setting.key)
      _  = a shouldEqual None
      a <- settings.all.toList
      _  = a shouldEqual Nil
      a <- settings.remove(setting.key)
      _  = a shouldEqual None

      a <- settings.set(setting.key, setting.value)
      _  = a shouldEqual None
      a <- settings.get(setting.key)
      _  = a shouldEqual setting.some
      a <- settings.setIfEmpty(setting.key, setting.value)
      _  = a shouldEqual setting.some
      a <- settings.get(setting.key)
      _  = a shouldEqual setting.some
      a <- settings.all.toList
      _  = a shouldEqual List(setting)

      a <- settings.remove(setting.key)
      _  = a shouldEqual setting.some
      a <- settings.get(setting.key)
      _  = a shouldEqual None
      a <- settings.all.toList
      _  = a shouldEqual Nil
      a <- settings.remove(setting.key)
      _  = a shouldEqual None
      a <- settings.setIfEmpty(setting.key, setting.value)
      _  = a shouldEqual None
    } yield {}
    val (state, _) = stateT.run(State.empty)
    state shouldEqual State(settings = Map((setting.key, setting)))
  }


  private val origin = Origin("origin")

  private val timestamp = Instant.now().truncatedTo(ChronoUnit.MILLIS)

  private val setting = Setting(key = "key", value = "value", timestamp = timestamp, origin = origin.some)

  private val settings = {

    val select = new SettingStatements.Select[StateT] {
      def apply(key: Key) = StateT { state =>
        val setting = state.settings.get(key)
        (state, setting)
      }
    }

    val insert = new SettingStatements.Insert[StateT] {
      def apply(setting: Setting) = {
        StateT { state =>
          val state1 = state.copy(settings = state.settings.updated(setting.key, setting))
          (state1, ())
        }
      }
    }

    val insertIfEmpty = new SettingStatements.InsertIfEmpty[StateT] {
      def apply(setting: Setting) = {
        StateT { state =>
          state.settings.get(setting.key).fold {
            val state1 = state.copy(settings = state.settings.updated(setting.key, setting))
            (state1, true)
          } { _ =>
            (state, false)
          }
        }
      }
    }

    val all = {
      val stateT = StateT { state =>
        val stream = Stream[StateT].apply(state.settings.values.toList)
        (state, stream)
      }
      Stream.lift(stateT).flatten
    }

    val delete = new SettingStatements.Delete[StateT] {
      def apply(key: Key) = {
        StateT { state =>
          val state1 = state.copy(settings = state.settings - key)
          (state1, ())
        }
      }
    }

    val statements = SettingsCassandra.Statements(
      select = select,
      insert = insert,
      insertIfEmpty = insertIfEmpty,
      all = all,
      delete = delete)

    implicit val clock = Clock.const[StateT](nanos = 0, millis = timestamp.toEpochMilli)

    implicit val measureDuration = MeasureDuration.fromClock(clock)

    SettingsCassandra[StateT](statements, origin.some)
      .withLog(Log.empty)
      .mapK(FunctionK.id[StateT], FunctionK.id[StateT])
  }


  case class State(settings: Map[Key, Setting])

  object State {
    val empty: State = State(Map.empty)
  }


  type StateT[A] = cats.data.StateT[Id, State, A]

  object StateT {

    def apply[A](f: State => (State, A)): StateT[A] = cats.data.StateT[Id, State, A](f)
  }
}