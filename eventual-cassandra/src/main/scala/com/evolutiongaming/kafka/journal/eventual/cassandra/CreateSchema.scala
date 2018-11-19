package com.evolutiongaming.kafka.journal.eventual.cassandra

import com.evolutiongaming.concurrent.async.Async
import com.evolutiongaming.concurrent.async.AsyncConverters._
import com.evolutiongaming.scassandra.{CreateKeyspaceIfNotExists, Session}

import scala.concurrent.ExecutionContext

object CreateSchema {

  def apply(
    schemaConfig: SchemaConfig,
    cassandraSync: CassandraSync[Async])(implicit ec: ExecutionContext /*TODO remove*/ , session: Session): Async[Tables] = {

    def createKeyspace() = {
      val keyspace = schemaConfig.keyspace
      if (keyspace.autoCreate) {
        val query = CreateKeyspaceIfNotExists(keyspace.name, keyspace.replicationStrategy)
        session.execute(query).async
      } else {
        Async.unit
      }
    }

    for {
      _ <- createKeyspace()
      tables <- cassandraSync {
        Tables(schemaConfig, session)
      }
    } yield tables
  }
}