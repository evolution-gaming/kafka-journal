package com.evolutiongaming.kafka.journal.eventual.cassandra

import cats.Monad
import cats.implicits._
import com.evolutiongaming.scassandra.CreateKeyspaceIfNotExists
import com.evolutiongaming.kafka.journal.eventual.cassandra.CassandraHelper._
import com.evolutiongaming.kafka.journal.util.FHelper._

object CreateSchema {

  def apply[F[_] : Monad : CassandraSession : CassandraSync](schemaConfig: SchemaConfig): F[Tables] = {

    def createKeyspace = {
      val keyspace = schemaConfig.keyspace
      if (keyspace.autoCreate) {
        val query = CreateKeyspaceIfNotExists(keyspace.name, keyspace.replicationStrategy)
        query.execute.unit
      } else {
        ().pure[F]
      }
    }

    for {
      _      <- createKeyspace
      tables <- CassandraSync[F].apply {
        Tables[F](schemaConfig)
      }
    } yield tables
  }
}