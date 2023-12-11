package com.evolutiongaming.kafka.journal.eventual.cassandra

import cats.implicits.catsStdInstancesForTry
import cats.syntax.all._
import com.datastax.driver.core.{Row, Statement}
import com.evolutiongaming.kafka.journal.{Setting, Settings}
import com.evolutiongaming.kafka.journal.util.StreamHelper._
import com.evolutiongaming.scassandra.TableName
import com.evolutiongaming.sstream.Stream
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.time.Instant
import scala.util.Try
import scala.util.control.NoStackTrace


class SetupJournalSchemaSpec extends AnyFunSuite with Matchers {
  import SetupSchemaSpec._

  test("migrate fresh") {
    val initial = State.empty
    val (state, _) = migrate(fresh = true)
      .run(initial)
      .get
    state shouldEqual initial.copy(
      version = "3".some,
      actions = List(
        Action.SyncEnd,
        Action.SetSetting("schema-version", "3"),
        Action.GetSetting("schema-version"),
        Action.SyncStart,
        Action.GetSetting("schema-version")))
  }

  test("migrate") {
    val initial = State.empty
    val (state, _) = migrate(fresh = false)
      .run(initial)
      .get
    state shouldEqual initial.copy(
      version = "3".some,
      actions = List(
        Action.SyncEnd,
        Action.SetSetting("schema-version", "3"),
        Action.Query,
        Action.SetSetting("schema-version", "2"),
        Action.Query,
        Action.SetSetting("schema-version", "1"),
        Action.Query,
        Action.SetSetting("schema-version", "0"),
        Action.Query,
        Action.GetSetting("schema-version"),
        Action.SyncStart,
        Action.GetSetting("schema-version")))
  }

  test("migrate 0") {
    val initial = State.empty.copy(version = "0".some)
    val (state, _) = migrate(fresh = false)
      .run(initial)
      .get
    state shouldEqual initial.copy(
      version = "3".some,
      actions = List(
        Action.SyncEnd,
        Action.SetSetting("schema-version", "3"),
        Action.Query,
        Action.SetSetting("schema-version", "2"),
        Action.Query,
        Action.SetSetting("schema-version", "1"),
        Action.Query,
        Action.GetSetting("schema-version"),
        Action.SyncStart,
        Action.GetSetting("schema-version")))
  }

  test("not migrate") {
    val initial = State.empty.copy(version = "3".some)
    val (state, _) = migrate(fresh = false)
      .run(initial)
      .get
    state shouldEqual initial.copy(actions = List(Action.GetSetting("schema-version")))
  }

  val timestamp: Instant = Instant.now()

  val schema: Schema = Schema(
    journal = TableName(keyspace = "journal", table = "journal"),
    metadata = TableName(keyspace = "journal", table = "metadata"),
    metaJournal = TableName(keyspace = "journal", table = "metaJournal"),
    pointer = TableName(keyspace = "journal", table = "pointer"),
    pointer2 = TableName(keyspace = "journal", table = "pointer2"),
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
          val state1 = state.copy(actions = Action.GetSetting(key) :: state.actions)
          (state1, setting)
        }
      }

      def set(key: K, value: V) = {
        StateT { state =>
          val setting = state.version.map { version => settingOf(key, version) }
          val state1 = state.copy(
            version = value.some,
            actions = Action.SetSetting(key, value) :: state.actions)
          (state1, setting)
        }
      }

      def setIfEmpty(key: K, value: V) = {
        StateT { state =>
          state.version match {
            case Some(version) =>
              val setting = settingOf(key, version)
              (state, setting.some)
            case None          =>
              val state1 = state.copy(
                version = value.some,
                actions = Action.SetSetting(key, value) :: state.actions)
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
      stateT.toStream.flatten
    }

    def unsafe = throw NotImplemented
  }

  implicit val cassandraSync: CassandraSync[StateT] = new CassandraSync[StateT] {

    def apply[A](fa: StateT[A]) = {
      StateT { state =>
        val state1 = state.add(Action.SyncStart)
        val (state2, a) = fa.run(state1).get
        val state3 = state2.add(Action.SyncEnd)
        (state3, a)
      }
    }
  }


  def migrate(fresh: Boolean): StateT[Unit] = {
    SetupJournalSchema(schema, fresh, settings, cassandraSync)
  }

  case class State(version: Option[String], actions: List[Action]) {

    def add(action: Action): State = copy(actions = action :: actions)
  }

  object State {
    val empty: State = State(version = None, actions = Nil)
  }


  type StateT[A] = cats.data.StateT[Try, State, A]

  object StateT {
    def apply[A](f: State => (State, A)): StateT[A] = cats.data.StateT[Try, State, A](s => Try(f(s)))
  }

  case object NotImplemented extends RuntimeException with NoStackTrace
}

object SetupSchemaSpec {
  sealed trait Action extends Product

  object Action {
    case object SyncStart extends Action

    case object SyncEnd extends Action

    case object Query extends Action

    final case class GetSetting(key: String) extends Action

    final case class SetSetting(key: String, value: String) extends Action
  }
}
