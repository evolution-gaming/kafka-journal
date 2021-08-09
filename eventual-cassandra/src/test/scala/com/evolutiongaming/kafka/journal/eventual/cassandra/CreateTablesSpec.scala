package com.evolutiongaming.kafka.journal.eventual.cassandra

import cats.Id
import cats.data.{NonEmptyList => Nel}
import cats.syntax.all._
import com.datastax.driver.core.{Row, Statement}
import com.evolutiongaming.catshelper.Log
import com.evolutiongaming.kafka.journal.eventual.cassandra.CreateTables.Table
import com.evolutiongaming.sstream.Stream
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import scala.util.control.NoStackTrace

class CreateTablesSpec extends AnyFunSuite with Matchers {

  test("create 1 table") {
    val initial = State.empty
    val (state, fresh) = createTables("keyspace", Nel.of(Table("table", "query"))).run(initial)
    fresh shouldEqual true
    state shouldEqual initial.copy(
      actions = List(
        Action.SyncEnd,
        Action.Query,
        Action.SyncStart,
        Action.Log("tables: table, fresh: true")))
  }

  test("create 2 tables and ignore 1") {
    val initial = State.empty.copy(tables = Set("table1"))
    val tables = Nel.of(
      Table("table1", "query"),
      Table("table2", "query"),
      Table("table3", "query"))
    val (state, fresh) = createTables("keyspace", tables).run(initial)
    fresh shouldEqual false
    state shouldEqual initial.copy(
      actions = List(
        Action.SyncEnd,
        Action.Query,
        Action.Query,
        Action.SyncStart,
        Action.Log("tables: table2,table3, fresh: false")))
  }

  test("create 2 tables") {
    val initial = State.empty.copy(tables = Set("table1"))
    val tables = Nel.of(
      Table("table1", "query"),
      Table("table2", "query"))
    val (state, fresh) = createTables("unknown", tables).run(initial)
    fresh shouldEqual true
    state shouldEqual initial.copy(
      actions = List(
        Action.SyncEnd,
        Action.Query,
        Action.Query,
        Action.SyncStart,
        Action.Log("tables: table1,table2, fresh: true")))
  }

  test("no create tables") {
    val initial = State.empty.copy(tables = Set("table"))
    val tables = Nel.of(Table("table", "query"))
    val (state, fresh) = createTables("keyspace", tables).run(initial)
    fresh shouldEqual false
    state shouldEqual initial
  }

  private val keyspaceMetadata = new KeyspaceMetadata[StateT] {
    def table(name: String) = {
      StateT { state =>
        val table = if (state.tables.contains(name)) TableMetadata(name).some else none[TableMetadata]
        (state, table)
      }
    }
  }


  private val cassandraMetadata = new CassandraMetadata[StateT] {
    def keyspace(name: String) = {
      StateT { state =>
        val keyspace = {
          if (state.keyspace == name) keyspaceMetadata.some
          else none[KeyspaceMetadata[StateT]]
        }
        (state, keyspace)
      }
    }
  }


  implicit val cassandraCluster: CassandraCluster[StateT] = new CassandraCluster[StateT] {

    def session = throw NotImplemented

    def metadata = cassandraMetadata.pure[StateT]
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

  private val log = new Log[StateT] {

    def debug(msg: => String, mdc: Log.Mdc) = ().pure[StateT]

    def info(msg: => String, mdc: Log.Mdc) = {
      StateT { state =>
        val state1 = state.add(Action.Log(msg))
        (state1, ())
      }
    }

    def warn(msg: => String, mdc: Log.Mdc) = ().pure[StateT]

    def warn(msg: => String, cause: Throwable, mdc: Log.Mdc) = ().pure[StateT]

    def error(msg: => String, mdc: Log.Mdc) = ().pure[StateT]

    def error(msg: => String, cause: Throwable, mdc: Log.Mdc) = ().pure[StateT]
  }


  private val createTables = CreateTables[StateT](log)

  case class State(keyspace: String, tables: Set[String], actions: List[Action]) {

    def add(action: Action): State = copy(actions = action :: actions)
  }

  object State {
    val empty: State = State("keyspace", Set.empty, List.empty)
  }


  type StateT[A] = cats.data.StateT[Id, State, A]

  object StateT {
    def apply[A](f: State => (State, A)): StateT[A] = cats.data.StateT[Id, State, A](f)
  }


  sealed trait Action extends Product

  object Action {
    case object Query extends Action
    case object SyncStart extends Action
    case object SyncEnd extends Action
    case class Log(msg: String) extends Action
  }

  object NotImplemented extends RuntimeException with NoStackTrace
}
