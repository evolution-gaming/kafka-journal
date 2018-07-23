package com.evolutiongaming.kafka.journal.eventual.cassandra

import com.evolutiongaming.cassandra.Session
import com.evolutiongaming.concurrent.async.Async
import com.evolutiongaming.concurrent.async.AsyncConverters._

import scala.concurrent.ExecutionContext

object CreateSchema {

  def apply(schemaConfig: SchemaConfig, session: Session)(implicit ec: ExecutionContext /*TODO remove*/): Async[Tables] = {

    def createKeyspace() = {
      val keyspace = schemaConfig.keyspace
      if (keyspace.autoCreate) {
        val query =
          s"""
             |CREATE KEYSPACE IF NOT EXISTS ${ keyspace.name }
             |WITH REPLICATION = { 'class' : ${ keyspace.replicationStrategy.asCql } }
             |""".stripMargin
        session.execute(query).async
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
