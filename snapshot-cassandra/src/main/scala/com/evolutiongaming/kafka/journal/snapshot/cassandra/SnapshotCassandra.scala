package com.evolutiongaming.kafka.journal.snapshot.cassandra

import cats.effect.kernel.{Async, Resource, Temporal}
import cats.effect.syntax.all._
import cats.syntax.all._
import cats.{Monad, MonadThrow, Parallel}
import com.evolutiongaming.catshelper.LogOf
import com.evolutiongaming.kafka.journal._
import com.evolutiongaming.kafka.journal.eventual.EventualPayloadAndType
import com.evolutiongaming.kafka.journal.eventual.cassandra.{CassandraCluster, CassandraConsistencyConfig, CassandraSession}
import com.evolutiongaming.scassandra.CassandraClusterOf

import java.time.Instant

object SnapshotCassandra {

  def of[F[_]: Async: Parallel: LogOf](
    config: SnapshotCassandraConfig,
    origin: Option[Origin],
    cassandraClusterOf: CassandraClusterOf[F]
  ): Resource[F, SnapshotStore[F]] = {

    def store(implicit cassandraCluster: CassandraCluster[F], cassandraSession: CassandraSession[F]) =
      of(config.schema, origin, config.consistencyConfig, config.numberOfSnapshots)

    for {
      cassandraCluster <- CassandraCluster.of[F](config.client, cassandraClusterOf, config.retries)
      cassandraSession <- cassandraCluster.session
      store <- store(cassandraCluster, cassandraSession).toResource
    } yield store

  }

  def of[F[_]: Temporal: Parallel: CassandraCluster: CassandraSession: LogOf](
    schemaConfig: SnapshotSchemaConfig,
    origin: Option[Origin],
    consistencyConfig: CassandraConsistencyConfig,
    numberOfSnapshots: Int
  ): F[SnapshotStore[F]] =
    for {
      schema <- SetupSnapshotSchema[F](schemaConfig, origin, consistencyConfig)
      statements <- Statements.of[F](schema, consistencyConfig)
    } yield SnapshotCassandra(statements, numberOfSnapshots)

  private sealed abstract class Main

  def apply[F[_]: MonadThrow](statements: Statements[F], numberOfSnapshots: Int): SnapshotStore[F] = {
    new Main with SnapshotStore[F] {

      def save(key: Key, snapshot: SnapshotRecord[EventualPayloadAndType]): F[Unit] = {
        statements.selectMetadata(key).flatMap {
          // such snapshot is already saved, overwrite
          case s if s.values.exists { case (seqNr, _) => snapshot.snapshot.seqNr == seqNr } =>
            update(key, s, snapshot)
          // there is a free place to add a snapshot
          case s if s.size < numberOfSnapshots => insert(key, s, snapshot)
          // all rows are taken, we have to update one of them
          case s => replace(key, s, snapshot)
        }
      }

      def insert(
        key: Key,
        savedSnapshots: Map[BufferNr, (SeqNr, Instant)],
        snapshot: SnapshotRecord[EventualPayloadAndType]
      ): F[Unit] = {
        val allBufferNrs = BufferNr.listOf(numberOfSnapshots)
        val takenBufferNrs = savedSnapshots.keySet
        val freeBufferNr = allBufferNrs.find(bufferNr => !takenBufferNrs.contains(bufferNr))
        MonadThrow[F].fromOption(freeBufferNr, SnapshotStoreError("Could not find a free key")).flatMap { bufferNr =>
          val wasApplied = statements.insertRecord(key, bufferNr, snapshot)
          wasApplied.flatMap { wasApplied =>
            // TODO: consider adding circuit breaker here
            if (wasApplied) ().pure[F] else save(key, snapshot)
          }
        }
      }

      def replace(
        key: Key,
        savedSnapshots: Map[BufferNr, (SeqNr, Instant)],
        insertSnapshot: SnapshotRecord[EventualPayloadAndType]
      ): F[Unit] = {
        val oldestSnapshot = savedSnapshots.toList.minByOption { case (_, (seqNr, timestamp)) => (seqNr, timestamp) }

        MonadThrow[F].fromOption(oldestSnapshot, SnapshotStoreError("Could not find an oldest snapshot")).flatMap {
          oldestSnapshot =>
            val (bufferNr, (deleteSnapshot, _)) = oldestSnapshot
            val wasApplied = statements.updateRecord(key, bufferNr, insertSnapshot, deleteSnapshot)
            wasApplied.flatMap { wasApplied =>
              // TODO: consider adding circuit breaker here
              if (wasApplied) ().pure[F] else save(key, insertSnapshot)
            }
        }
      }

      def update(
        key: Key,
        savedSnapshots: Map[BufferNr, (SeqNr, Instant)],
        insertSnapshot: SnapshotRecord[EventualPayloadAndType]
      ): F[Unit] = {
        val sortedSnapshots = savedSnapshots.toList.sortBy { case (_, (seqNr, timestamp)) => (seqNr, timestamp) }

        val olderSnapshot =
          sortedSnapshots.find { case (_, (seqNr, _)) => seqNr == insertSnapshot.snapshot.seqNr }
        MonadThrow[F].fromOption(olderSnapshot, SnapshotStoreError("Could not find snapshot with seqNr")).flatMap {
          olderSnapshot =>
            val (bufferNr, (deleteSnapshot, _)) = olderSnapshot
            val wasApplied = statements.updateRecord(key, bufferNr, insertSnapshot, deleteSnapshot)
            wasApplied.flatMap { wasApplied =>
              // TODO: consider adding circuit breaker here
              if (wasApplied) ().pure[F] else save(key, insertSnapshot)
            }
        }
      }

      def load(key: Key, criteria: SnapshotSelectionCriteria): F[Option[SnapshotRecord[EventualPayloadAndType]]] =
        for {
          savedSnapshots <- statements.selectMetadata(key)
          sortedSnapshots = savedSnapshots.toList.sortBy { case (_, (seqNr, timestamp)) => (seqNr, timestamp) }
          bufferNr = sortedSnapshots.reverse.collectFirst {
            case (bufferNr, (seqNr, timestamp))
                if seqNr >= criteria.minSeqNr &&
                  seqNr <= criteria.maxSeqNr &&
                  timestamp.compareTo(criteria.minTimestamp) >= 0 &&
                  timestamp.compareTo(criteria.maxTimestamp) <= 0 =>
              bufferNr
          }
          snapshot <- bufferNr.flatTraverse(statements.selectRecords(key, _))
        } yield snapshot

      def delete(key: Key, criteria: SnapshotSelectionCriteria): F[Unit] =
        for {
          savedSnapshots <- statements.selectMetadata(key)
          bufferNrs = savedSnapshots.toList.collect {
            case (bufferNr, (seqNr, timestamp))
                if seqNr >= criteria.minSeqNr &&
                  seqNr <= criteria.maxSeqNr &&
                  timestamp.compareTo(criteria.minTimestamp) >= 0 &&
                  timestamp.compareTo(criteria.maxTimestamp) <= 0 =>
              bufferNr
          }
          _ <- bufferNrs.traverse(statements.deleteRecords(key, _))
        } yield ()

      def delete(key: Key, seqNr: SeqNr): F[Unit] =
        delete(key, SnapshotSelectionCriteria.one(seqNr))

    }
  }

  final case class Statements[F[_]](
    insertRecord: SnapshotStatements.InsertRecord[F],
    updateRecord: SnapshotStatements.UpdateRecord[F],
    selectRecords: SnapshotStatements.SelectRecord[F],
    selectMetadata: SnapshotStatements.SelectMetadata[F],
    deleteRecords: SnapshotStatements.Delete[F]
  )

  object Statements {
    def of[F[_]: Monad: CassandraSession](schema: SnapshotSchema, consistencyConfig: CassandraConsistencyConfig): F[Statements[F]] = {
      for {
        insertRecord <- SnapshotStatements.InsertRecord.of[F](schema.snapshot, consistencyConfig.write)
        updateRecord <- SnapshotStatements.UpdateRecord.of[F](schema.snapshot, consistencyConfig.write)
        selectRecord <- SnapshotStatements.SelectRecord.of[F](schema.snapshot, consistencyConfig.read)
        selectMetadata <- SnapshotStatements.SelectMetadata.of[F](schema.snapshot, consistencyConfig.read)
        deleteRecords <- SnapshotStatements.Delete.of[F](schema.snapshot, consistencyConfig.write)
      } yield Statements(insertRecord, updateRecord, selectRecord, selectMetadata, deleteRecords)
    }
  }

}
