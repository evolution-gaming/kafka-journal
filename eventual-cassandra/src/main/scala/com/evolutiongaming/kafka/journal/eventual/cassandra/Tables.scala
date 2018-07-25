package com.evolutiongaming.kafka.journal.eventual.cassandra

import com.evolutiongaming.cassandra.Session
import com.evolutiongaming.concurrent.async.Async
import com.evolutiongaming.concurrent.async.AsyncConverters._

import scala.concurrent.ExecutionContext

final case class Tables(journal: TableName, metadata: TableName, pointer: TableName)


object Tables {

  def apply(schemaConfig: SchemaConfig, session: Session)(implicit ec: ExecutionContext /*TODO remove*/): Async[Tables] = {

    val keyspace = schemaConfig.keyspace.name

    def apply(name: TableName, query: String) = {
      if (schemaConfig.autoCreate) {
        for {
          _ <- session.execute(query).async
        } yield {
          name
        }
      } else {
        name.async
      }
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

    for {
      journal <- journal
      metadata <- metadata
      pointer <- pointer
    } yield {
      Tables(journal = journal, metadata = metadata, pointer = pointer)
    }
  }
}