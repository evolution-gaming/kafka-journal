package com.evolutiongaming.kafka.journal.eventual.cassandra

import java.time.Instant

import cats.{Id, Monad}
import cats.syntax.all._
import com.datastax.driver.core.{Row, Statement}
import com.evolutiongaming.kafka.journal.util.TestSync
import com.evolutiongaming.kafka.journal.{Setting, Settings}
import com.evolutiongaming.scassandra.TableName
import com.evolutiongaming.sstream.Stream
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import scala.util.control.NoStackTrace


class SetupSchemaSpec extends AnyFunSuite with Matchers {

  test("migrate fresh") {
    val initial = State.empty
    val (state, _) = migrate(fresh = true).run(initial)
    state shouldEqual initial.copy(version = "1".some)
  }

  test("migrate") {
    val initial = State.empty
    val (state, _) = migrate(fresh = false).run(initial)
    state shouldEqual initial.copy(
      version = "1".some,
      actions = List(Action.SyncEnd, Action.Query, Action.Query, Action.SyncStart))
  }

  test("migrate 0") {
    val initial = State.empty.copy(version = "0".some)
    val (state, _) = migrate(fresh = false).run(initial)
    state shouldEqual initial.copy(
      version = "1".some,
      actions = List(Action.SyncEnd, Action.Query, Action.SyncStart))
  }

  test("not migrate") {
    val initial = State.empty.copy(version = "1".some)
    val (state, _) = migrate(fresh = false).run(initial)
    state shouldEqual initial
  }

  val timestamp: Instant = Instant.now()

  val schema: Schema = Schema(
    journal = TableName(keyspace = "journal", table = "journal"),
    metadata = TableName(keyspace = "journal", table = "metadata"),
    metaJournal = TableName(keyspace = "journal", table = "metaJournal"),
    pointer = TableName(keyspace = "journal", table = "pointer"),
    setting = TableName(keyspace = "journal", table = "setting"))

  implicit val settings: Settings[StateT] = {

    def settingOf(key: Setting.Key, value: Setting.Value) = Setting(key, value, timestamp, None)

    new Settings[StateT] {

      def get(key: K) = {
        StateT { state =>
          val setting = for {
            version <- state.version
          } yield {
            settingOf(key, version)
          }
          (state, setting)
        }
      }

      def set(key: K, value: V) = {
        StateT { state =>
          val setting = state.version.map { version => settingOf(key, version) }
          (state.copy(version = value.some), setting)
        }
      }

      def setIfEmpty(key: K, value: V) = {
        StateT { state =>
          state.version match {
            case Some(version) =>
              val setting = settingOf(key, version)
              (state, setting.some)
            case None          =>
              val state1 = state.copy(version = value.some)
              (state1, none[Setting])
          }
        }
      }

      def remove(key: K) = throw NotImplemented

      def all = throw NotImplemented
    }
  }

  implicit val cassandraSession: CassandraSession[StateT] = new CassandraSession[StateT] {

    def prepare(query: String) = throw NotImplemented

    def execute(statement: Statement) = {
      val stateT = StateT { state =>
        val state1 = state.add(Action.Query)
        val rows = Stream.empty[StateT, Row]
        (state1, rows)
      }
      Stream.lift(stateT).flatten
    }

    def unsafe = throw NotImplemented
  }

  implicit val cassandraSync: CassandraSync[StateT] = new CassandraSync[StateT] {

    def apply[A](fa: StateT[A]) = {
      StateT { state =>
        val state1 = state.add(Action.SyncStart)
        val (state2, a) = fa.run(state1)
        val state3 = state2.add(Action.SyncEnd)
        (state3, a)
      }
    }
  }


  def migrate(fresh: Boolean)(implicit monad: Monad[StateT]): StateT[Unit] = {
    implicit val sync = TestSync[StateT](monad)
    SetupSchema.migrate[StateT](schema, fresh, settings, cassandraSync)
  }

  case class State(version: Option[String], actions: List[Action]) {

    def add(action: Action): State = copy(actions = action :: actions)
  }

  object State {
    val empty: State = State(version = None, actions = Nil)
  }


  type StateT[A] = cats.data.StateT[Id, State, A]

  object StateT {
    def apply[A](f: State => (State, A)): StateT[A] = cats.data.StateT[Id, State, A](f)
  }


  sealed trait Action extends Product

  object Action {
    case object SyncStart extends Action
    case object SyncEnd extends Action
    case object Query extends Action
  }


  case object NotImplemented extends RuntimeException with NoStackTrace
}
