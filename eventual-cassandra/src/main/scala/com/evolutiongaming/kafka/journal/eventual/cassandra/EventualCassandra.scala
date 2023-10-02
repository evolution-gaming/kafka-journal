package com.evolutiongaming.kafka.journal.eventual.cassandra

import cats.effect.kernel.{Async, Temporal}
import cats.effect.syntax.resource._
import cats.effect.{Concurrent, Resource}
import cats.syntax.all._
import cats.{MonadThrow, Parallel}
import com.evolutiongaming.catshelper.{LogOf, MeasureDuration, ToTry}
import com.evolutiongaming.kafka.journal._
import com.evolutiongaming.kafka.journal.eventual._
import com.evolutiongaming.kafka.journal.eventual.cassandra.EventualCassandraConfig.ConsistencyConfig
import com.evolutiongaming.kafka.journal.util.CatsHelper._
import com.evolutiongaming.scassandra.util.FromGFuture
import com.evolutiongaming.kafka.journal.util.StreamHelper._
import com.evolutiongaming.scassandra.{CassandraClusterOf, TableName}
import com.evolutiongaming.skafka.{Offset, Partition, Topic}
import com.evolutiongaming.sstream.Stream

/** Creates read-only representation of the data stored to Cassandra.
  *
  * It is intended to be used for journal recovery.
  *
  * One may not be able to use Kafka only to recover because Kafka may have the
  * retention window expired for the older journals, or the offset might be too
  * far in the past for such recovery to be practical (i.e. too much irrelevant
  * data will have to be filtered out).
  *
  * Hence, one is to use [[EventualCassandra]] to recover the tail of the
  * journal and then read the newest data from Kafka.
  */
object EventualCassandra {

  /** Creates [[EventualJournal]] instance for a given Cassandra cluster factory.
    *
    * Underlying schema is automatically created or migrated if required.
    */
  def of[
    F[_]
    : Async
    : Parallel
    : ToTry: LogOf
    : FromGFuture
    : MeasureDuration
    : JsonCodec.Decode
  ](
    config: EventualCassandraConfig,
    origin: Option[Origin],
    metrics: Option[EventualJournal.Metrics[F]],
    cassandraClusterOf: CassandraClusterOf[F],
  ): Resource[F, EventualJournal[F]] = {

    def journal(implicit cassandraCluster: CassandraCluster[F], cassandraSession: CassandraSession[F]) = {
      of(config.schema, origin, metrics, config.consistencyConfig)
    }

    for {
      cassandraCluster <- CassandraCluster.of[F](config.client, cassandraClusterOf, config.retries)
      cassandraSession <- cassandraCluster.session
      journal          <- journal(cassandraCluster, cassandraSession).toResource
    } yield journal
  }

  /** Creates [[EventualJournal]] instance for a given Cassandra session.
    *
    * Underlying schema is automatically created or migrated if required.
    */
  def of[
    F[_]
    : Temporal
    : Parallel
    : ToTry: LogOf
    : CassandraCluster: CassandraSession
    : MeasureDuration
    : JsonCodec.Decode
  ](
    schemaConfig: SchemaConfig,
    origin: Option[Origin],
    metrics: Option[EventualJournal.Metrics[F]],
    consistencyConfig: ConsistencyConfig
  ): F[EventualJournal[F]] = {

    for {
      log          <- LogOf[F].apply(EventualCassandra.getClass)
      schema       <- SetupSchema[F](schemaConfig, origin, consistencyConfig)
      segmentNrsOf  = SegmentNrsOf[F](first = Segments.default, second = Segments.old)
      statements   <- Statements.of(schema, segmentNrsOf, Segments.default, consistencyConfig.read)
    } yield {
      val journal = apply[F](statements).withLog(log)
      metrics
        .fold(journal) { metrics => journal.withMetrics(metrics) }
        .enhanceError
    }
  }


  private sealed abstract class Main

  /** Creates [[EventualJournal]] instance calling Cassandra appropriately.
    *
    * The implementation itself is abstracted from the calls to Cassandra which
    * should be passed as part of [[Statements]] parameter.
    */
  def apply[F[_]: MonadThrow](statements: Statements[F]): EventualJournal[F] = {

    new Main with EventualJournal[F] {

      def pointer(key: Key) = {
        statements
          .metaJournal
          .journalPointer(key)
      }

      def read(key: Key, from: SeqNr) = {

        def read(statement: JournalStatements.SelectRecords[F], head: JournalHead) = {

          def read(from: SeqNr) = {

            def read(from: SeqNr, segment: Segment) = {
              val range = SeqRange(from, SeqNr.max)
              statement(key, segment.nr, range).map { record => (record, segment) }
            }

            read(from, Segment(from, head.segmentSize))
              .chain { case (record, segment) =>
                for {
                  from    <- record.seqNr.next[Option]
                  segment <- segment.next(from)
                } yield {
                  read(from, segment)
                }
              }
              .map { case (record, _) => record }
              .stateful(from) { case (seqNr, record) =>
                if (seqNr <= record.seqNr) {
                  val seqNr1 = record
                    .seqNr
                    .next[Option]
                  (seqNr1, Stream[F].single(record))
                } else {
                  val msg = s"Data integrity violated: seqNr $seqNr duplicated in multiple records from eventual journal, key $key"
                  val err = new JournalError(msg)
                  (seqNr.some, err.raiseError[F, EventRecord[EventualPayloadAndType]].toStream)
                }
              }
          }


          head.deleteTo match {
            case None           => read(from)
            case Some(deleteTo) =>
              if (from > deleteTo.value) read(from)
              else deleteTo.value.next[Option] match {
                case Some(from) => read(from)
                case None       => Stream.empty[F, EventRecord[EventualPayloadAndType]]
              }
          }
        }

        for {
          journalHead <- statements.metaJournal.journalHead(key).toStream
          result      <- journalHead match {
            case Some(journalHead) => read(statements.records, journalHead)
            case None              => Stream.empty[F, EventRecord[EventualPayloadAndType]]
          }
        } yield result
      }

      def ids(topic: Topic) = {
        statements.metaJournal.ids(topic)
      }

      def offset(topic: Topic, partition: Partition): F[Option[Offset]] = {
        for {
          offset <- statements.selectOffset2(topic, partition)
          offset <- offset.fold { statements.selectOffset(topic, partition) } { _.some.pure[F] }
        } yield offset
      }
    }
  }


  final case class Statements[F[_]](
    records: JournalStatements.SelectRecords[F],
    metaJournal: MetaJournalStatements[F],
    selectOffset: PointerStatements.SelectOffset[F],
    selectOffset2: Pointer2Statements.SelectOffset[F])

  object Statements {

    def apply[F[_]](implicit F: Statements[F]): Statements[F] = F

    def of[F[_]: Concurrent: CassandraSession: ToTry: JsonCodec.Decode](
      schema: Schema,
      segmentNrsOf: SegmentNrsOf[F],
      segments: Segments,
      consistencyConfig: ConsistencyConfig.Read
    ): F[Statements[F]] = {
      for {
        selectRecords  <- JournalStatements.SelectRecords.of[F](schema.journal, consistencyConfig)
        metaJournal    <- MetaJournalStatements.of(schema, segmentNrsOf, segments, consistencyConfig)
        selectOffset   <- PointerStatements.SelectOffset.of[F](schema.pointer, consistencyConfig)
        selectOffset2  <- Pointer2Statements.SelectOffset.of[F](schema.pointer2, consistencyConfig)
      } yield {
        Statements(selectRecords, metaJournal, selectOffset, selectOffset2)
      }
    }
  }


  trait MetaJournalStatements[F[_]] {

    def journalHead(key: Key): F[Option[JournalHead]]

    def journalPointer(key: Key): F[Option[JournalPointer]]

    def ids(topic: Topic): Stream[F, String]
  }

  object MetaJournalStatements {

    def of[F[_]: Concurrent: CassandraSession](
      schema: Schema,
      segmentNrsOf: SegmentNrsOf[F],
      segments: Segments,
      consistencyConfig: ConsistencyConfig.Read
    ): F[MetaJournalStatements[F]] = {
      of(schema.metaJournal, segmentNrsOf, segments, consistencyConfig)
    }

    def of[F[_]: Concurrent: CassandraSession](
      metaJournal: TableName,
      segmentNrsOf: SegmentNrsOf[F],
      segments: Segments,
      consistencyConfig: ConsistencyConfig.Read
    ): F[MetaJournalStatements[F]] = {
      for {
        selectJournalHead    <- cassandra.MetaJournalStatements.SelectJournalHead.of[F](metaJournal, consistencyConfig)
        selectJournalPointer <- cassandra.MetaJournalStatements.SelectJournalPointer.of[F](metaJournal, consistencyConfig)
        selectIds            <- cassandra.MetaJournalStatements.SelectIds.of[F](metaJournal, consistencyConfig)
      } yield {
        fromMetaJournal(segmentNrsOf, selectJournalHead, selectJournalPointer, selectIds, segments)
      }
    }

    def fromMetaJournal[F[_]: Concurrent](
      segmentNrsOf: SegmentNrsOf[F],
      journalHead: cassandra.MetaJournalStatements.SelectJournalHead[F],
      journalPointer: cassandra.MetaJournalStatements.SelectJournalPointer[F],
      ids: cassandra.MetaJournalStatements.SelectIds[F],
      segments: Segments
    ): MetaJournalStatements[F] = {

      val journalHead1 = journalHead
      val journalPointer1 = journalPointer
      val ids1 = ids

      def firstOrSecond[A](key: Key)(f: SegmentNr => F[Option[A]]): F[Option[A]] = {
        for {
          segmentNrs <- segmentNrsOf(key)
          result     <- f(segmentNrs.first).orElsePar { segmentNrs.second.flatTraverse(f) }
        } yield result
      }

      new Main with MetaJournalStatements[F] {

        def journalHead(key: Key) = {
          firstOrSecond(key) {  segmentNr => journalHead1(key, segmentNr) }
        }

        def journalPointer(key: Key) = {
          firstOrSecond(key) {  segmentNr => journalPointer1(key, segmentNr) }
        }

        def ids(topic: Topic) = {
          for {
            segmentNr <- segments.segmentNrs.toStream1[F]
            id        <- ids1(topic, segmentNr)
          } yield id
        }
      }
    }
  }
}

