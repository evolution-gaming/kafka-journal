package com.evolution.kafka.journal.eventual.cassandra

import cats.effect.kernel.{Async, Temporal}
import cats.effect.syntax.all.*
import cats.effect.{Concurrent, Resource}
import cats.syntax.all.*
import cats.{MonadThrow, Parallel}
import com.evolution.kafka.journal.Journal.DataIntegrityConfig
import com.evolution.kafka.journal.cassandra.CassandraConsistencyConfig
import com.evolution.kafka.journal.eventual.*
import com.evolution.kafka.journal.eventual.cassandra.JournalStatements.JournalRecord
import com.evolution.kafka.journal.util.CatsHelper.*
import com.evolution.kafka.journal.util.StreamHelper.*
import com.evolution.kafka.journal.{cassandra as _, *}
import com.evolutiongaming.catshelper.{Log, LogOf, MeasureDuration, ToTry}
import com.evolutiongaming.scassandra.util.FromGFuture
import com.evolutiongaming.scassandra.{CassandraClusterOf, TableName}
import com.evolutiongaming.skafka.{Offset, Partition, Topic}
import com.evolutiongaming.sstream.Stream

/**
 * Creates read-only representation of the data stored to Cassandra.
 *
 * It is intended to be used for journal recovery.
 *
 * One may not be able to use Kafka only to recover because Kafka may have the retention window
 * expired for the older journals, or the offset might be too far in the past for such recovery to
 * be practical (i.e. too much irrelevant data will have to be filtered out).
 *
 * Hence, one is to use [[EventualCassandra]] to recover the tail of the journal and then read the
 * newest data from Kafka.
 */
object EventualCassandra {

  /**
   * Creates [[EventualJournal]] instance for a given Cassandra cluster factory.
   *
   * Underlying schema is automatically created or migrated if required.
   */
  def make[F[_]: Async: Parallel: ToTry: LogOf: FromGFuture: MeasureDuration: JsonCodec.Decode](
    config: EventualCassandraConfig,
    origin: Option[Origin],
    metrics: Option[EventualJournal.Metrics[F]],
    cassandraClusterOf: CassandraClusterOf[F],
    dataIntegrity: DataIntegrityConfig,
  ): Resource[F, EventualJournal[F]] = {

    def journal(implicit
      cassandraCluster: CassandraCluster[F],
      cassandraSession: CassandraSession[F],
    ) = {
      of(config.schema, origin, metrics, config.consistencyConfig, dataIntegrity)
    }

    for {
      cassandraCluster <- CassandraCluster.make[F](config.client, cassandraClusterOf, config.retries)
      cassandraSession <- cassandraCluster.session
      journal <- journal(cassandraCluster, cassandraSession).toResource
    } yield journal
  }

  /**
   * Creates [[EventualJournal]] instance for a given Cassandra session.
   *
   * Underlying schema is automatically created or migrated if required.
   */
  def of[F[_]: Temporal: Parallel: ToTry: LogOf: CassandraCluster: CassandraSession: MeasureDuration: JsonCodec.Decode](
    schemaConfig: SchemaConfig,
    origin: Option[Origin],
    metrics: Option[EventualJournal.Metrics[F]],
    consistencyConfig: CassandraConsistencyConfig,
    dataIntegrity: DataIntegrityConfig,
  ): F[EventualJournal[F]] = {

    for {
      log <- LogOf[F].apply(EventualCassandra.getClass)
      schema <- SetupSchema[F](schemaConfig, origin, consistencyConfig)
      segmentNrsOf = SegmentNrs.Of[F](first = Segments.default, second = Segments.old)
      statements <- Statements.of(schema, segmentNrsOf, Segments.default, consistencyConfig.read)
      _ <- log.info(s"kafka-journal version: ${ Version.current.value }")
    } yield {
      implicit val log1: Log[F] = log
      val journal = apply[F](statements, dataIntegrity).withLog(log)
      metrics
        .fold(journal) { metrics => journal.withMetrics(metrics) }
        .enhanceError
    }
  }

  private sealed abstract class Main

  /**
   * Creates [[EventualJournal]] instance calling Cassandra appropriately.
   *
   * The implementation itself is abstracted from the calls to Cassandra which should be passed as
   * part of [[Statements]] parameter.
   */
  def apply[F[_]: MonadThrow: Log](statements: Statements[F], dataIntegrity: DataIntegrityConfig)
    : EventualJournal[F] = {

    new Main with EventualJournal[F] {

      def pointer(key: Key): F[Option[JournalPointer]] = {
        statements
          .metaJournal
          .journalPointer(key)
      }

      def read(key: Key, from: SeqNr): Stream[F, EventRecord[EventualPayloadAndType]] = {

        def read(statement: JournalStatements.SelectRecords[F], head: JournalHead) = {

          def read(from: SeqNr) = {

            def read(from: SeqNr, segment: Segment) = {
              val range = SeqRange(from, SeqNr.max)
              statement(key, segment.nr, range).map { record => (record, segment) }
            }

            val records =
              read(from, Segment.journal(from, head.segmentSize))
                .chain {
                  case (record, segment) =>
                    for {
                      from <- record.event.seqNr.next[Option]
                      segment <- segment.next(from)
                    } yield {
                      read(from, segment)
                    }
                }
                .map { case (record, _) => record }

            val records1 = {
              head.recordId.fold { records } { fromMeta =>
                records.flatMap { event =>
                  event.metaRecordId match {

                    case None if dataIntegrity.correlateEventsWithMeta =>
                      for {
                        _ <- Log[F].error(s"Data integrity violated: event $event is orphan, key $key").toStream
                        r <- Stream.empty[F, JournalRecord]
                      } yield r

                    case None =>
                      Log[F]
                        .warn(s"Disabled data integrity violated: event $event is orphan, key $key")
                        .toStream
                        .as(event)

                    case Some(fromEvent) if fromEvent == fromMeta =>
                      Stream.single(event)

                    case Some(_) if dataIntegrity.correlateEventsWithMeta =>
                      for {
                        _ <-
                          Log[F].error(s"Data integrity violated: event $event belongs to purged meta, key $key").toStream
                        r <- Stream.empty[F, JournalRecord]
                      } yield r

                    case Some(_) =>
                      Log[F]
                        .warn(s"Disabled data integrity violated: event $event belongs to purged meta, key $key")
                        .toStream
                        .as(event)

                  }
                }
              }
            }

            val records2 =
              if (dataIntegrity.seqNrUniqueness) {
                records1
                  .stateful(from) {
                    case (seqNr, record) =>
                      if (seqNr <= record.event.seqNr) {
                        val seqNr1 = record
                          .event
                          .seqNr
                          .next[Option]
                        (seqNr1, Stream[F].single(record))
                      } else {
                        val msg =
                          s"Data integrity violated: seqNr $seqNr duplicated in multiple records from eventual journal, key $key"
                        val err = new JournalError(msg)
                        (seqNr.some, err.raiseError[F, JournalRecord].toStream)
                      }
                  }
              } else {
                records1
              }

            records2.map(_.event)
          }

          head.deleteTo match {
            case None => read(from)
            case Some(deleteTo) =>
              if (from > deleteTo.value) read(from)
              else
                deleteTo.value.next[Option] match {
                  case Some(from) => read(from)
                  case None => Stream.empty[F, EventRecord[EventualPayloadAndType]]
                }
          }
        }

        for {
          journalHead <- statements.metaJournal.journalHead(key).toStream
          result <- journalHead match {
            case Some(journalHead) => read(statements.records, journalHead)
            case None => Stream.empty[F, EventRecord[EventualPayloadAndType]]
          }
        } yield result
      }

      def ids(topic: Topic): Stream[F, Topic] = {
        statements.metaJournal.ids(topic)
      }

      def offset(topic: Topic, partition: Partition): F[Option[Offset]] = {
        statements.selectOffset2(topic, partition)
      }
    }
  }

  private[journal] final case class Statements[F[_]](
    records: JournalStatements.SelectRecords[F],
    metaJournal: MetaJournalStatements[F],
    selectOffset2: Pointer2Statements.SelectOffset[F],
  )

  private[journal] object Statements {

    def apply[F[_]](
      implicit
      F: Statements[F],
    ): Statements[F] = F

    def of[F[_]: Concurrent: CassandraSession: ToTry: JsonCodec.Decode](
      schema: Schema,
      segmentNrsOf: SegmentNrs.Of[F],
      segments: Segments,
      consistencyConfig: CassandraConsistencyConfig.Read,
    ): F[Statements[F]] = {
      for {
        selectRecords <- JournalStatements.SelectRecords.of[F](schema.journal, consistencyConfig)
        metaJournal <- MetaJournalStatements.of(schema, segmentNrsOf, segments, consistencyConfig)
        selectOffset2 <- Pointer2Statements.SelectOffset.of[F](schema.pointer2, consistencyConfig)
      } yield {
        Statements(selectRecords, metaJournal, selectOffset2)
      }
    }
  }

  private[journal] trait MetaJournalStatements[F[_]] {

    def journalHead(key: Key): F[Option[JournalHead]]

    def journalPointer(key: Key): F[Option[JournalPointer]]

    def ids(topic: Topic): Stream[F, String]
  }

  private[journal] object MetaJournalStatements {

    def of[F[_]: Concurrent: CassandraSession](
      schema: Schema,
      segmentNrsOf: SegmentNrs.Of[F],
      segments: Segments,
      consistencyConfig: CassandraConsistencyConfig.Read,
    ): F[MetaJournalStatements[F]] = {
      of(schema.metaJournal, segmentNrsOf, segments, consistencyConfig)
    }

    def of[F[_]: Concurrent: CassandraSession](
      metaJournal: TableName,
      segmentNrsOf: SegmentNrs.Of[F],
      segments: Segments,
      consistencyConfig: CassandraConsistencyConfig.Read,
    ): F[MetaJournalStatements[F]] = {
      for {
        selectJournalHead <- cassandra.MetaJournalStatements.SelectJournalHead.of[F](metaJournal, consistencyConfig)
        selectJournalPointer <-
          cassandra.MetaJournalStatements.SelectJournalPointer.of[F](metaJournal, consistencyConfig)
        selectIds <- cassandra.MetaJournalStatements.SelectIds.of[F](metaJournal, consistencyConfig)
      } yield {
        fromMetaJournal(segmentNrsOf, selectJournalHead, selectJournalPointer, selectIds, segments)
      }
    }

    def fromMetaJournal[F[_]: Concurrent](
      segmentNrsOf: SegmentNrs.Of[F],
      journalHead: cassandra.MetaJournalStatements.SelectJournalHead[F],
      journalPointer: cassandra.MetaJournalStatements.SelectJournalPointer[F],
      ids: cassandra.MetaJournalStatements.SelectIds[F],
      segments: Segments,
    ): MetaJournalStatements[F] = {

      val journalHead1 = journalHead
      val journalPointer1 = journalPointer
      val ids1 = ids

      def firstOrSecond[A](key: Key)(f: SegmentNr => F[Option[A]]): F[Option[A]] = {
        for {
          segmentNrs <- segmentNrsOf.metaJournal(key)
          first = f(segmentNrs.first)
          result <- segmentNrs
            .second
            .fold {
              first
            } { second =>
              first.orElsePar { f(second) }
            }
        } yield result
      }

      new Main with MetaJournalStatements[F] {

        def journalHead(key: Key): F[Option[JournalHead]] = {
          firstOrSecond(key) { segmentNr => journalHead1(key, segmentNr) }
        }

        def journalPointer(key: Key): F[Option[JournalPointer]] = {
          firstOrSecond(key) { segmentNr => journalPointer1(key, segmentNr) }
        }

        def ids(topic: Topic): Stream[F, Topic] = {
          for {
            segmentNr <- segments.metaJournalSegmentNrs.toStream1[F]
            id <- ids1(topic, segmentNr)
          } yield id
        }
      }
    }
  }
}
