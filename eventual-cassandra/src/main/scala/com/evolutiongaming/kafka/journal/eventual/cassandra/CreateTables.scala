package com.evolutiongaming.kafka.journal.eventual.cassandra

import cats.Monad
import cats.implicits._
import com.evolutiongaming.kafka.journal.{Log, LogOf}
import com.evolutiongaming.nel.Nel

trait CreateTables[F[_]] {
  import CreateTables.Table

  def apply(keyspace: String, tables: Nel[Table]): F[Unit]
}


object CreateTables { self =>

  def apply[F[_]](implicit F: CreateTables[F]): CreateTables[F] = F


  def apply[F[_] : Monad : CassandraCluster : CassandraSession : CassandraSync](
    log: Log[F]
  ): CreateTables[F] = new CreateTables[F] {

    def apply(keyspace: String, tables: Nel[Table]) = {

      def missing(keyspace: KeyspaceMetadata[F]) = {
        for {
          tables <- tables.toList.foldLeftM(List.empty[Table]) { (create, table) =>
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

      def create(tables: List[Table]) = {
        for {
          _      <- log.info(tables.map(_.name).mkString(", "))
          result <- CassandraSync[F].apply {
            tables.foldMapM { table =>
              CassandraSession[F].execute(table.query).void
            }
          }
        } yield result
      }

      for {
        tables   <- tables.distinct.pure[F]
        metadata <- CassandraCluster[F].metadata
        keyspace <- metadata.keyspace(keyspace)
        tables   <- keyspace.fold(tables.toList.pure[F])(missing)
        result   <- if (tables.isEmpty) ().pure[F] else create(tables)
      } yield result
    }
  }


  def of[F[_] : Monad : CassandraCluster : CassandraSession : CassandraSync : LogOf]: F[CreateTables[F]] = {
    for {
      log <- LogOf[F].apply(self.getClass)
    } yield {
      apply[F](log)
    }
  }


  final case class Table(name: String, query: String)
}