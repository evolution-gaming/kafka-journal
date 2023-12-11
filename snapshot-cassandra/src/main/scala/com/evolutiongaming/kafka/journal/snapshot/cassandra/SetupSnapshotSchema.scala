package com.evolutiongaming.kafka.journal.snapshot.cassandra

import cats.Parallel
import cats.effect.kernel.Temporal
import cats.syntax.all._
import com.evolutiongaming.catshelper.LogOf
import com.evolutiongaming.kafka.journal.Origin
import com.evolutiongaming.kafka.journal.eventual.cassandra.EventualCassandraConfig.ConsistencyConfig
import com.evolutiongaming.kafka.journal.eventual.cassandra.{CassandraCluster, CassandraSession, CassandraSync, SettingsCassandra, SetupSchema}

object SetupSnapshotSchema {

  val SettingKey = "snapshot-schema-version"

  def apply[F[_]: Temporal: Parallel: CassandraCluster: CassandraSession: LogOf](
    config: SnapshotSchemaConfig,
    origin: Option[Origin],
    consistencyConfig: ConsistencyConfig
  ): F[SnapshotSchema] = {

    def createSchema(implicit cassandraSync: CassandraSync[F]) = CreateSnapshotSchema(config)

    for {
      cassandraSync <- CassandraSync.of[F](config.keyspace, config.locksTable, origin)
      ab <- createSchema(cassandraSync)
      (schema, fresh) = ab
      settings <- SettingsCassandra.of[F](schema.setting, origin, consistencyConfig)
      setupSchema = SetupSchema[F](cassandraSync, settings, SettingKey, Nil)
      _ <- setupSchema.migrate(fresh)
    } yield schema
  }
}
