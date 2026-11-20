package com.evolution.kafka.journal.eventual.cassandra

import cats.Functor
import cats.syntax.all.*
import com.evolution.scassandra

import scala.jdk.OptionConverters.*

trait CassandraMetadata[F[_]] {

  def keyspace(name: String): F[Option[KeyspaceMetadata[F]]]
}

object CassandraMetadata {

  /**
   * Reads the metadata of the session.
   *
   * The Java driver 4 provides immutable snapshots of the metadata, so each call takes a fresh
   * snapshot from the session, i.e. the tables created after a keyspace metadata was obtained are
   * visible.
   */
  def apply[F[_]: Functor](session: scassandra.CassandraSession[F]): CassandraMetadata[F] = new CassandraMetadata[F] {

    def keyspace(name: String): F[Option[KeyspaceMetadata[F]]] = {
      for {
        metadata <- session.metadata
      } yield
        for {
          _ <- metadata.getKeyspace(name).toScala
        } yield {
          KeyspaceMetadata[F](session, name)
        }
    }
  }
}

trait KeyspaceMetadata[F[_]] {

  def table(name: String): F[Option[TableMetadata]]
}

object KeyspaceMetadata {

  def apply[F[_]: Functor](session: scassandra.CassandraSession[F], keyspace: String): KeyspaceMetadata[F] =
    new KeyspaceMetadata[F] {

      def table(name: String): F[Option[TableMetadata]] = {
        for {
          metadata <- session.metadata
        } yield
          for {
            keyspace <- metadata.getKeyspace(keyspace).toScala
            table <- keyspace.getTable(name).toScala
          } yield {
            TableMetadata(table.getName.asInternal)
          }
      }
    }
}

final case class TableMetadata(name: String)
