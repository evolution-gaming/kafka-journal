package com.evolutiongaming.kafka.journal.eventual.cassandra

import cats.Monad
import cats.implicits._
import com.evolutiongaming.kafka.journal.eventual.cassandra.CassandraHelper._
import com.evolutiongaming.scassandra.TableName

final case class Tables(journal: TableName, metadata: TableName, pointer: TableName)


object Tables {

  def apply[F[_] : Monad : CassandraSession : CassandraSync](schemaConfig: SchemaConfig): F[Tables] = {

    val keyspace = schemaConfig.keyspace.name
    val autoCreate = schemaConfig.autoCreate

    def apply(name: TableName, query: String) = {
      if (autoCreate) query.execute.as(name) else name.pure[F]
    }

    def tableName(name: String) = TableName(keyspace = keyspace, table = name)

    val journal = {
      val name = tableName(schemaConfig.journalTable)
      val query = JournalStatement.createTable(name)
      apply(name, query)
    }

    val metadata = {
      val name = tableName(schemaConfig.metadataTable)
      val query = MetadataStatement.createTable(name)
      apply(name, query)
    }

    val pointer = {
      val name = tableName(schemaConfig.pointerTable)
      val query = PointerStatement.createTable(name)
      apply(name, query)
    }

    val result = for {
      journal <- journal
      metadata <- metadata
      pointer <- pointer
    } yield {
      Tables(journal = journal, metadata = metadata, pointer = pointer)
    }

    if (autoCreate) CassandraSync[F].apply(result) else result
  }
}