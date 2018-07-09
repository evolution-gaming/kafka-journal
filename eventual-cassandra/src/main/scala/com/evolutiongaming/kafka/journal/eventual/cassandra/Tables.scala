package com.evolutiongaming.kafka.journal.eventual.cassandra

import com.datastax.driver.core.Session
import com.evolutiongaming.cassandra.CassandraHelper._

import scala.concurrent.{ExecutionContext, Future}

case class Tables(journal: TableName, metadata: TableName, pointer: TableName)


object Tables {

  def apply(schemaConfig: SchemaConfig, session: Session)(implicit ec: ExecutionContext /*TODO remove*/): Future[Tables] = {

    val keyspace = schemaConfig.keyspace.name

    def apply(name: TableName, query: String) = {
      if (schemaConfig.autoCreate) {
        for {
          _ <- session.executeAsync(query).asScala()
        } yield {
          name
        }
      } else {
        Future.successful(name)
      }
    }

    def tableName(name: String) = TableName(keyspace = keyspace, table = name)

    val journal = {
      val name = tableName(schemaConfig.journalName)
      val query = JournalStatement.createTable(name)
      apply(name, query)
    }

    val metadata = {
      val name = tableName(schemaConfig.metadataName)
      val query = MetadataStatement.createTable(name)
      apply(name, query)
    }

    val pointer = {
      val name = tableName(schemaConfig.pointerName)
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