package com.evolutiongaming.kafka.journal.snapshot.cassandra

import cats.Parallel
import cats.effect.kernel.Temporal
import cats.syntax.all._
import com.evolutiongaming.catshelper.LogOf
import com.evolutiongaming.kafka.journal.Origin
import com.evolutiongaming.kafka.journal.eventual.cassandra.{CassandraCluster, CassandraConsistencyConfig, CassandraSession, CassandraSync, SettingsCassandra}

/** Creates a new schema */
object SetupSnapshotSchema {

  def apply[F[_]: Temporal: Parallel: CassandraCluster: CassandraSession: LogOf](
    config: SnapshotSchemaConfig,
    origin: Option[Origin],
    consistencyConfig: CassandraConsistencyConfig
  ): F[SnapshotSchema] = {

    def createSchema(implicit cassandraSync: CassandraSync[F]) = CreateSnapshotSchema(config)

    for {
      cassandraSync <- CassandraSync.of[F](config.keyspace, config.locksTable, origin)
      ab <- createSchema(cassandraSync)
      (schema, fresh) = ab
      _ <- SettingsCassandra.of[F](schema.setting, origin, consistencyConfig)
    } yield schema
  }

}
