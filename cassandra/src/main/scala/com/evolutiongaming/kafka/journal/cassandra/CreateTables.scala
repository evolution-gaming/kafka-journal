package com.evolutiongaming.kafka.journal.cassandra

import cats.data.NonEmptyList as Nel
import cats.syntax.all.*
import cats.{Monad, Order}
import com.evolutiongaming.catshelper.{Log, LogOf}
import com.evolutiongaming.kafka.journal.cassandra.CassandraSync
import com.evolutiongaming.kafka.journal.eventual.cassandra.CassandraHelper.*
import com.evolutiongaming.kafka.journal.eventual.cassandra.{CassandraCluster, CassandraSession, KeyspaceMetadata}

/** Creates tables in a specific keyspace  */
private[journal] trait CreateTables[F[_]] {
  import CreateTables.{Fresh, Table}

  def apply(keyspace: String, tables: Nel[Table]): F[Fresh]
}

private[journal] object CreateTables { self =>

  /** `true` if all tables passed to `CreateTables` did not exist and were created */
  type Fresh = Boolean

  def apply[F[_]](implicit F: CreateTables[F]): CreateTables[F] = F

  def apply[F[_]: Monad: CassandraCluster: CassandraSession: CassandraSync](
      log: Log[F],
  ): CreateTables[F] = new CreateTables[F] {

    def apply(keyspace: String, tables: Nel[Table]) = {

      def missing(keyspace: KeyspaceMetadata[F]) = {
        for {
          tables <- tables.foldLeftM(List.empty[Table]) { (create, table) =>
            for {
              metadata <- keyspace.table(table.name)
            } yield {
              metadata.fold(table :: create) { _ => create }
            }
          }
        } yield {
          tables.reverse
        }
      }

      def create(tables1: Nel[Table]) = {
        val fresh = tables1.length === tables.length
        for {
          _ <- log.info(s"tables: ${tables1.map(_.name).mkString_(",")}, fresh: $fresh")
          _ <- CassandraSync[F].apply { tables1.foldMapM { _.queries.foldMapM { _.execute.first.void } } }
        } yield fresh
      }

      for {
        tables   <- tables.distinct.pure[F]
        metadata <- CassandraCluster[F].metadata
        keyspace <- metadata.keyspace(keyspace)
        tables1  <- keyspace.fold(tables.toList.pure[F])(missing)
        fresh    <- tables1.toNel.fold(false.pure[F])(create)
      } yield fresh
    }
  }

  def of[F[_]: Monad: CassandraCluster: CassandraSession: CassandraSync: LogOf]: F[CreateTables[F]] = {
    for {
      log <- LogOf[F].apply(self.getClass)
    } yield {
      apply[F](log)
    }
  }

  def const[F[_]](fresh: F[Fresh]): CreateTables[F] = (_: String, _: Nel[Table]) => fresh

  /** Table to be created in a specific keyspace.
    *
    * @param name
    *   Table name (without a keyspace) used to check if table already exists.
    * @param queries
    *   CQL statements (including keyspace) used to create a table. I.e. it
    *   could contain `CREATE TABLE` statement and also related `CREATE INDEX`
    *   statements.
    */
  final case class Table(name: String, queries: Nel[String])

  object Table {

    implicit val orderTable: Order[Table] = Order.by { (a: Table) => a.name }

    def apply(name: String, query: String): Table = Table(name, Nel.of(query))
  }
}
