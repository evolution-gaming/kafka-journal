package com.evolutiongaming.kafka.journal.eventual.cassandra

import cats.effect.kernel.Temporal
import cats.syntax.all._
import cats.{Monad, MonadThrow, Parallel}
import com.evolutiongaming.catshelper.LogOf
import com.evolutiongaming.kafka.journal.eventual.EventualPayloadAndType
import com.evolutiongaming.kafka.journal.eventual.cassandra.EventualCassandraConfig.ConsistencyConfig
import com.evolutiongaming.kafka.journal.{Key, Origin, SeqNr, SnapshotRecord, SnapshotStoreFlat}

import java.time.Instant

object SnapshotCassandra {

  def of[F[_]: Temporal: Parallel: CassandraCluster: CassandraSession: LogOf](
    schemaConfig: SchemaConfig,
    origin: Option[Origin],
    consistencyConfig: ConsistencyConfig
  ): F[SnapshotStoreFlat[F]] =
    for {
      schema <- SetupSchema[F](schemaConfig, origin, consistencyConfig)
      statements <- Statements.of[F](schema, consistencyConfig)
    } yield SnapshotCassandra(statements)

  private sealed abstract class Main

  def apply[F[_]: MonadThrow](statements: Statements[F]): SnapshotStoreFlat[F] = {
    new Main with SnapshotStoreFlat[F] {

      def save(key: Key, snapshot: SnapshotRecord[EventualPayloadAndType]): F[Unit] = ???
      def load(key: Key, maxSeqNr: SeqNr, maxTimestamp: Instant, minSeqNr: SeqNr, minTimestamp: Instant): F[Unit] = ???
      def drop(key: Key, maxSeqNr: SeqNr, maxTimestamp: Instant, minSeqNr: SeqNr, minTimestamp: Instant): F[Unit] = ???
      def drop(key: Key, seqNr: SeqNr): F[Unit] = ???

    }
  }

  final case class Statements[F[_]](
    insertRecords: SnapshotStatements.InsertRecord[F],
    selectRecords: SnapshotStatements.SelectRecord[F],
    selectMetadata: SnapshotStatements.SelectMetadata[F],
    deleteRecords: SnapshotStatements.Delete[F]
  )

  object Statements {
    def of[F[_]: Monad: CassandraSession](schema: Schema, consistencyConfig: ConsistencyConfig): F[Statements[F]] = {
      for {
        insertRecords <- SnapshotStatements.InsertRecord.of[F](schema.snapshot, consistencyConfig.write)
        selectRecord <- SnapshotStatements.SelectRecord.of[F](schema.snapshot, consistencyConfig.read)
        selectMetadata <- SnapshotStatements.SelectMetadata.of[F](schema.snapshot, consistencyConfig.read)
        deleteRecords <- SnapshotStatements.Delete.of[F](schema.snapshot, consistencyConfig.write)
      } yield Statements(insertRecords, selectRecord, selectMetadata, deleteRecords)
    }
  }

}
