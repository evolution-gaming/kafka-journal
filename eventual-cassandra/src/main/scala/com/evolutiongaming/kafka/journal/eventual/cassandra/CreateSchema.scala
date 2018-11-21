package com.evolutiongaming.kafka.journal.eventual.cassandra

import com.evolutiongaming.kafka.journal.IO
import com.evolutiongaming.kafka.journal.IO.ops._
import com.evolutiongaming.scassandra.CreateKeyspaceIfNotExists

object CreateSchema {

  def apply[F[_] : IO : CassandraSession](
    schemaConfig: SchemaConfig,
    cassandraSync: CassandraSync[F]): F[Tables] = {

    def createKeyspace() = {
      val keyspace = schemaConfig.keyspace
      if (keyspace.autoCreate) {
        val query = CreateKeyspaceIfNotExists(keyspace.name, keyspace.replicationStrategy)
        CassandraSession[F].execute(query)
      } else {
        IO[F].unit
      }
    }

    for {
      _ <- createKeyspace()
      tables <- cassandraSync {
        Tables(schemaConfig, CassandraSession[F])
      }
    } yield tables
  }
}