package com.evolutiongaming.kafka.journal.cassandra

import cats.data.{State, NonEmptyList as Nel}
import cats.syntax.all.*
import com.datastax.driver.core.{PreparedStatement, Row, Statement}
import com.evolutiongaming.catshelper.Log
import com.evolutiongaming.kafka.journal.eventual.cassandra.{CassandraCluster, CassandraMetadata, CassandraSession, KeyspaceMetadata, TableMetadata}
import com.evolutiongaming.kafka.journal.util.StreamHelper.*
import com.evolutiongaming.sstream.Stream
import org.scalatest.funsuite.AnyFunSuite

import scala.util.control.NoStackTrace
import CreateTables.Table
import cats.effect.Resource
import com.evolutiongaming.scassandra

class CreateTablesSpec extends AnyFunSuite {

  type F[A] = State[Database, A]

  test("create 1 table") {
    val program           = createTables("keyspace", Nel.of(Table("table", "query")))
    val (database, fresh) = program.run(Database.empty).value
    assert(fresh)
    assert(
      database.actions == List(
        Action.Log("tables: table, fresh: true"),
        Action.SyncStart,
        Action.Query,
        Action.SyncEnd,
      ),
    )
  }

  test("create 2 tables and ignore 1") {
    val tables            = Nel.of(Table("table1", "query"), Table("table2", "query"), Table("table3", "query"))
    val program           = createTables("keyspace", tables)
    val (database, fresh) = program.run(Database.withTables("table1")).value
    assert(!fresh)
    assert(
      database.actions == List(
        Action.Log("tables: table2,table3, fresh: false"),
        Action.SyncStart,
        Action.Query,
        Action.Query,
        Action.SyncEnd,
      ),
    )
  }

  test("create 2 tables") {
    val tables            = Nel.of(Table("table1", "query"), Table("table2", "query"))
    val program           = createTables("unknown", tables)
    val (database, fresh) = program.run(Database.withTables("table1")).value
    assert(fresh)
    assert(
      database.actions == List(
        Action.Log("tables: table1,table2, fresh: true"),
        Action.SyncStart,
        Action.Query,
        Action.Query,
        Action.SyncEnd,
      ),
    )
  }

  test("no create tables") {
    val tables            = Nel.of(Table("table", "query"))
    val program           = createTables("keyspace", tables)
    val (database, fresh) = program.run(Database.withTables("table")).value
    assert(!fresh)
    assert(database.actions.isEmpty)
  }

  private val keyspaceMetadata = new KeyspaceMetadata[F] {
    def table(name: String) =
      Database.tableExists(name).map { exists =>
        Option.when(exists)(TableMetadata(name))
      }
  }

  private val cassandraMetadata = new CassandraMetadata[F] {
    def keyspace(name: String) =
      Database.keyspaceExists(name).map { exists =>
        Option.when(exists)(keyspaceMetadata)
      }
  }

  implicit val cassandraCluster: CassandraCluster[F] = new CassandraCluster[F] {

    def session: Resource[F, CassandraSession[F]] = throw NotImplemented

    def metadata: F[CassandraMetadata[F]] = cassandraMetadata.pure[F]
  }

  implicit val cassandraSession: CassandraSession[F] = new CassandraSession[F] {

    def prepare(query: String): F[PreparedStatement] = throw NotImplemented

    def execute(statement: Statement): Stream[F, Row] =
      Database.query.as(Stream.empty[F, Row]).toStream.flatten

    def unsafe: scassandra.CassandraSession[F] = throw NotImplemented
  }

  implicit val cassandraSync: CassandraSync[F] = new CassandraSync[F] {

    def apply[A](fa: F[A]): F[A] =
      Database.syncStart *> fa <* Database.syncEnd

  }

  private val log = new Log[F] {

    def info(msg: => String, mdc: Log.Mdc) = Database.log(msg)

    def trace(msg: => String, mdc: Log.Mdc)                   = ().pure[F]
    def debug(msg: => String, mdc: Log.Mdc)                   = ().pure[F]
    def warn(msg: => String, mdc: Log.Mdc)                    = ().pure[F]
    def warn(msg: => String, cause: Throwable, mdc: Log.Mdc)  = ().pure[F]
    def error(msg: => String, mdc: Log.Mdc)                   = ().pure[F]
    def error(msg: => String, cause: Throwable, mdc: Log.Mdc) = ().pure[F]
  }

  private val createTables = CreateTables[F](log)

  case class Database(keyspace: String, tables: Set[String], actions: List[Action]) {

    def syncStart: Database = add(Action.SyncStart)
    def syncEnd: Database   = add(Action.SyncEnd)

    def query: Database = add(Action.Query)

    def keyspaceExists(name: String): Boolean = keyspace == name
    def tableExists(name: String): Boolean    = tables.contains(name)

    def log(msg: String): Database = add(Action.Log(msg))

    private def add(action: Action): Database =
      copy(actions = actions :+ action)

  }

  object Database {

    val empty: Database                       = Database("keyspace", Set.empty, List.empty)
    def withTables(tables: String*): Database = empty.copy(tables = tables.toSet)

    def syncStart: F[Unit] = State.modify(_.syncStart)
    def syncEnd: F[Unit]   = State.modify(_.syncEnd)

    def query: F[Unit] = State.modify(_.query)

    def keyspaceExists(name: String): F[Boolean] = State.inspect(_.keyspaceExists(name))
    def tableExists(name: String): F[Boolean]    = State.inspect(_.tableExists(name))

    def log(msg: String): F[Unit] = State.modify(_.log(msg))

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
