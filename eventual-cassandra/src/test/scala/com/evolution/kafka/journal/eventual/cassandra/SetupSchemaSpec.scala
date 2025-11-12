package com.evolution.kafka.journal.eventual.cassandra

import cats.implicits.catsStdInstancesForTry
import cats.syntax.all.*
import com.datastax.driver.core.{PreparedStatement, RegularStatement, Row, Statement}
import com.evolution.kafka.journal.cassandra.CassandraSync
import com.evolution.kafka.journal.util.StreamHelper.*
import com.evolution.kafka.journal.{Setting, Settings}
import com.evolutiongaming.scassandra
import com.evolutiongaming.scassandra.TableName
import com.evolutiongaming.sstream.Stream
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.time.Instant
import scala.util.Try
import scala.util.control.NoStackTrace

class SetupSchemaSpec extends AnyFunSuite with Matchers {
  import SetupSchemaSpec.*

  test("do not apply any migration on fully auto-created keyspace") {
    val initial = State.empty
    val (state, _) = migrate(fresh = true)
      .run(initial)
      .get
    state shouldEqual initial.copy(
      version = "8".some,
      actions = List(
        Action.SyncEnd,
        Action.SetSetting("schema-version", "8"),
        Action.GetSetting("schema-version"),
        Action.SyncStart,
        Action.GetSetting("schema-version"),
      ),
    )
  }

  test("apply migrations on previously created keyspace") {
    val initial = State.empty
    val (state, _) = migrate(fresh = false)
      .run(initial)
      .get
    state shouldEqual initial.copy(
      version = "8".some,
      actions = List(
        Action.SyncEnd,
        Action.SetSetting("schema-version", "8"),
        Action.Query("DROP TABLE IF EXISTS journal.pointer"),
        Action.SetSetting("schema-version", "7"),
        Action.Query("DROP TABLE IF EXISTS journal.metadata"),
        Action.SetSetting("schema-version", "6"),
        Action.Query("DROP TABLE IF EXISTS pointer"),
        Action.SetSetting("schema-version", "5"),
        Action.Query("ALTER TABLE journal.metaJournal ADD record_id UUID"),
        Action.SetSetting("schema-version", "4"),
        Action.Query("ALTER TABLE journal.journal ADD meta_record_id UUID"),
        Action.SetSetting("schema-version", "3"),
        Action.Query("""
          |CREATE TABLE IF NOT EXISTS journal.pointer2 (
          |topic text,
          |partition int,
          |offset bigint,
          |created timestamp,
          |updated timestamp,
          |PRIMARY KEY ((topic, partition)))
          |""".stripMargin),
        Action.SetSetting("schema-version", "2"),
        Action.Query("DROP TABLE IF EXISTS metadata"),
        Action.SetSetting("schema-version", "1"),
        Action.Query("ALTER TABLE journal.journal ADD version TEXT"),
        Action.SetSetting("schema-version", "0"),
        Action.Query("ALTER TABLE journal.journal ADD headers map<text, text>"),
        Action.GetSetting("schema-version"),
        Action.SyncStart,
        Action.GetSetting("schema-version"),
      ),
    )
  }

  test("apply the last few missing migrations (after 6th to latest)") {
    val initial = State.empty.copy(version = "6".some)
    val (state, _) = migrate(fresh = false)
      .run(initial)
      .get
    state shouldEqual initial.copy(
      version = "8".some,
      actions = List(
        Action.SyncEnd,
        Action.SetSetting("schema-version", "8"),
        Action.Query("DROP TABLE IF EXISTS journal.pointer"),
        Action.SetSetting("schema-version", "7"),
        Action.Query("DROP TABLE IF EXISTS journal.metadata"),
        Action.GetSetting("schema-version"),
        Action.SyncStart,
        Action.GetSetting("schema-version"),
      ),
    )
  }

  test("do not apply any migration when newer version has been already applied") {
    val initial = State.empty.copy(version = "987654321".some)
    val (state, _) = migrate(fresh = false)
      .run(initial)
      .get
    state shouldEqual initial.copy(
      version = "987654321".some,
      actions = List(
        Action.GetSetting("schema-version"),
      ),
    )
  }

  test("do not apply any migration script when all migrations have been already applied before") {
    val initial = State.empty.copy(version = "8".some)
    val (state, _) = migrate(fresh = false)
      .run(initial)
      .get
    state shouldEqual initial.copy(actions = List(Action.GetSetting("schema-version")))
  }

  val timestamp: Instant = Instant.now()

  val keyspace = "journal"
  val schema: Schema = Schema(
    keyspaceName = keyspace,
    journal = TableName(keyspace = keyspace, table = "journal"),
    metaJournal = TableName(keyspace = keyspace, table = "metaJournal"),
    pointer2 = TableName(keyspace = keyspace, table = "pointer2"),
    setting = TableName(keyspace = keyspace, table = "setting"),
  )

  implicit val settings: Settings[StateT] = {

    def settingOf(key: Setting.Key, value: Setting.Value) = Setting(key, value, timestamp, None)

    new Settings[StateT] {

      def get(key: K): StateT[Option[Setting]] = {
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

      def set(key: K, value: V): StateT[Option[Setting]] = {
        StateT { state =>
          val setting = state.version.map { version => settingOf(key, version) }
          val state1 = state.copy(version = value.some, actions = Action.SetSetting(key, value) :: state.actions)
          (state1, setting)
        }
      }

      def remove(key: K): StateT[Option[Setting]] = throw NotImplemented

      def all: Stream[StateT, Setting] = throw NotImplemented
    }
  }

  implicit val cassandraSession: CassandraSession[StateT] = new CassandraSession[StateT] {

    def prepare(query: String): StateT[PreparedStatement] = throw NotImplemented

    def execute(statement: Statement): Stream[StateT, Row] = {
      val stateT = StateT { state =>
        val query = statement match {
          case statement: RegularStatement => statement.getQueryString
          case other => sys.error(s"Unexpected statement type: $other")
        }
        val state1 = state.add(Action.Query(query))
        val rows = Stream.empty[StateT, Row]
        (state1, rows)
      }
      stateT.toStream.flatten
    }

    def unsafe: scassandra.CassandraSession[StateT] = throw NotImplemented
  }

  implicit val cassandraSync: CassandraSync[StateT] = new CassandraSync[StateT] {

    def apply[A](fa: StateT[A]): StateT[A] = {
      StateT { state =>
        val state1 = state.add(Action.SyncStart)
        val (state2, a) = fa.run(state1).get
        val state3 = state2.add(Action.SyncEnd)
        (state3, a)
      }
    }
  }

  def migrate(fresh: Boolean): StateT[Unit] = {
    SetupSchema.migrate[StateT](schema, fresh, settings, cassandraSync)
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

    final case class Query(query: String) extends Action

    final case class GetSetting(key: String) extends Action

    final case class SetSetting(key: String, value: String) extends Action
  }
}
