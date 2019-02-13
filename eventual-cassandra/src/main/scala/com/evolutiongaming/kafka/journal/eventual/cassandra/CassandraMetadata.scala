package com.evolutiongaming.kafka.journal.eventual.cassandra

import cats.effect.Sync
import cats.implicits._
import com.evolutiongaming.scassandra

trait CassandraMetadata[F[_]] {

  def keyspace(name: String): F[Option[KeyspaceMetadata[F]]]
}

object CassandraMetadata {

  def apply[F[_] : Sync](metadata: scassandra.Metadata): CassandraMetadata[F] = new CassandraMetadata[F] {

    def keyspace(name: String) = {
      for {
        keyspace <- Sync[F].delay { metadata.keyspace(name) }
      } yield for {
        keyspace <- keyspace
      } yield {
        KeyspaceMetadata[F](keyspace)
      }
    }
  }
}


trait KeyspaceMetadata[F[_]] {

  def table(name: String): F[Option[TableMetadata]]
}

object KeyspaceMetadata {

  def apply[F[_] : Sync](metadata: scassandra.KeyspaceMetadata): KeyspaceMetadata[F] = new KeyspaceMetadata[F] {

    def table(name: String) = {
      for {
        table <- Sync[F].delay { metadata.table(name) }
      } yield for {
        table <- table
      } yield {
        TableMetadata(table.name)
      }
    }
  }
}


final case class TableMetadata(name: String)