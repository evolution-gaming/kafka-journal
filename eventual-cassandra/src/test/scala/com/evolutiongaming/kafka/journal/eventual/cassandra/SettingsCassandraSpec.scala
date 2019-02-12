package com.evolutiongaming.kafka.journal.eventual.cassandra

import java.time.Instant
import java.time.temporal.ChronoUnit

import cats.implicits._
import com.evolutiongaming.kafka.journal.Setting.Key
import com.evolutiongaming.kafka.journal.stream.Stream
import com.evolutiongaming.kafka.journal.{ClockOf, HostName, Setting}
import org.scalatest.{FunSuite, Matchers}

class SettingsCassandraSpec extends FunSuite with Matchers {

  test("set") {
    val (state, prev) = settings.set(setting.key, setting.value).run(State.Empty)
    state shouldEqual State(settings = Map((setting.key, setting)))
    prev shouldEqual None
  }

  test("get") {
    val (state, value) = settings.get(setting.key).run(State.Empty)
    state shouldEqual State.Empty
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
    state shouldEqual State.Empty
    prev shouldEqual Some(setting)
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
      _  = a shouldEqual Some(setting)
      a <- settings.all.toList
      _  = a shouldEqual List(setting)

      a <- settings.remove(setting.key)
      _  = a shouldEqual Some(setting)
      a <- settings.get(setting.key)
      _  = a shouldEqual None
      a <- settings.all.toList
      _  = a shouldEqual Nil
      a <- settings.remove(setting.key)
      _  = a shouldEqual None
    } yield {}
    val (state, _) = stateT.run(State.Empty)
    state shouldEqual State.Empty
  }


  private val origin = "hostName"

  private val timestamp = Instant.now().truncatedTo(ChronoUnit.MILLIS)

  private val setting = Setting(key = "key", value = "value", timestamp = timestamp, origin = Some(origin))

  private val settings = {

    val select = new SettingStatement.Select[StateT] {
      def apply(key: Key) = StateT { state =>
        val setting = state.settings.get(key)
        (state, setting)
      }
    }

    val all = StateT { state =>
      val stream = Stream[StateT].apply(state.settings.values.toList)
      (state, stream)
    }

    val insert = new SettingStatement.Insert[StateT] {
      def apply(setting: Setting) = {
        StateT { state =>
          val state1 = state.copy(settings = state.settings.updated(setting.key, setting))
          (state1, ())
        }
      }
    }

    val delete = new SettingStatement.Delete[StateT] {
      def apply(key: Key) = {
        StateT { state =>
          val state1 = state.copy(settings = state.settings - key)
          (state1, ())
        }
      }
    }

    val statements = SettingsCassandra.Statements(
      select = select,
      all = all,
      insert = insert,
      delete = delete)

    implicit val clock = ClockOf[StateT](timestamp.toEpochMilli)

    SettingsCassandra[StateT](statements, Some(HostName("hostName")))
  }


  case class State(settings: Map[Key, Setting])

  object State {
    val Empty: State = State(Map.empty)
  }


  type StateT[A] = cats.data.StateT[cats.Id, State, A]

  object StateT {

    def apply[A](f: State => (State, A)): StateT[A] = cats.data.StateT[cats.Id, State, A](f)
  }
}