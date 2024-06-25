package com.evolutiongaming.kafka.journal.eventual.cassandra

import cats.data.{NonEmptyList => Nel}
import cats.syntax.all._
import cats.{Monad, Order}
import com.evolutiongaming.catshelper.{Log, LogOf}
import com.evolutiongaming.kafka.journal.cassandra.{CreateTables => CreateTables2}
import com.evolutiongaming.kafka.journal.eventual.cassandra.{CassandraCluster, CassandraSession}

/** Creates tables in a specific keyspace */
@deprecated(
  since = "3.3.9",
  message = "Use a class from `com.evolutiongaming.kafka.journal.cassandra` (without `eventual` part) package instead",
)
trait CreateTables[F[_]] {
  import CreateTables.{Fresh, Table}

  def apply(keyspace: String, tables: Nel[Table]): F[Fresh]
}

@deprecated(
  since = "3.3.9",
  message = "Use a class from `com.evolutiongaming.kafka.journal.cassandra` (without `eventual` part) package instead",
)
object CreateTables { self =>

  /** `true` if all tables passed to `CreateTables` did not exist and were created */
  type Fresh = CreateTables2.Fresh

  private[cassandra] def apply[F[_]](createTables2: CreateTables2[F]): CreateTables[F] = { (keyspace, tables) =>
    createTables2(
      keyspace = keyspace,
      tables = tables.map { table =>
        CreateTables2.Table(table.name, table.queries)
      },
    )
  }

  def apply[F[_]](implicit F: CreateTables[F]): CreateTables[F] = F

  def apply[F[_]: Monad: CassandraCluster: CassandraSession: CassandraSync](log: Log[F]): CreateTables[F] = {
    implicit val cassandraSync2 = CassandraSync[F].toCassandraSync2
    val createTables2           = CreateTables2(log)
    CreateTables(createTables2)
  }

  def of[F[_]: Monad: CassandraCluster: CassandraSession: CassandraSync: LogOf]: F[CreateTables[F]] = {
    implicit val cassandraSync2 = CassandraSync[F].toCassandraSync2
    val createTables2           = CreateTables2.of[F]
    createTables2.map(CreateTables(_))
  }

  def const[F[_]](fresh: F[Fresh]): CreateTables[F] = {
    val createTables2 = CreateTables2.const(fresh)
    CreateTables(createTables2)
  }

  /** Table to be created in a specific keyspace.
    *
    * @param name
    *   Table name (without a keyspace) used to check if table already exists.
    * @param queries
    *   CQL statements (including keyspace) used to create a table. I.e. it could contain `CREATE TABLE` statement and
    *   also related `CREATE INDEX` statements.
    */
  final case class Table(name: String, queries: Nel[String])

  object Table {

    implicit val orderTable: Order[Table] =
      Order[CreateTables2.Table].contramap { table =>
        CreateTables2.Table(name = table.name, queries = table.queries)
      }

    def apply(name: String, query: String): Table = Table(name, Nel.of(query))
  }
}
