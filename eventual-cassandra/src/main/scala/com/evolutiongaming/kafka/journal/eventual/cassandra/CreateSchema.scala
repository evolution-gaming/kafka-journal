package com.evolutiongaming.kafka.journal.eventual.cassandra

import com.datastax.driver.core.Session
import com.evolutiongaming.cassandra.CassandraHelper._
import com.evolutiongaming.concurrent.async.Async
import com.evolutiongaming.concurrent.async.AsyncConverters._

import scala.concurrent.ExecutionContext

object CreateSchema {

  def apply(schemaConfig: SchemaConfig, session: Session)(implicit ec: ExecutionContext /*TODO remove*/): Async[Tables] = {

    def createKeyspace() = {
      val keyspace = schemaConfig.keyspace
      if (keyspace.autoCreate) {
        val query = JournalStatement.createKeyspace(keyspace)
        session.executeAsync(query).asScala().async
      } else {
        Async.unit
      }
    }

    for {
      _ <- createKeyspace()
      tables <- Tables(schemaConfig, session)
    } yield tables
  }
}
