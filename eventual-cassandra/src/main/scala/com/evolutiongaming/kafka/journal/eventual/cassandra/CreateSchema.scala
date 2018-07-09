package com.evolutiongaming.kafka.journal.eventual.cassandra

import com.datastax.driver.core.Session
import com.evolutiongaming.cassandra.CassandraHelper._

import scala.concurrent.{ExecutionContext, Future}

object CreateSchema {

  def apply(schemaConfig: SchemaConfig, session: Session)(implicit ec: ExecutionContext /*TODO remove*/): Future[Tables] = {

    def createKeyspace() = {
      val keyspace = schemaConfig.keyspace
      if (keyspace.autoCreate) {
        val query = JournalStatement.createKeyspace(keyspace)
        session.executeAsync(query).asScala()
      } else {
        Future.unit
      }
    }

    for {
      _ <- createKeyspace()
      tables <- Tables(schemaConfig, session)
    } yield tables
  }
}
