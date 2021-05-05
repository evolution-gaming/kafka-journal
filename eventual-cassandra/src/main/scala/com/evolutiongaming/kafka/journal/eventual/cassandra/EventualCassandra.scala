package com.evolutiongaming.kafka.journal.eventual.cassandra

import cats.effect.{Concurrent, Resource, Timer}
import cats.syntax.all._
import cats.{Monad, Parallel}
import com.evolutiongaming.catshelper.CatsHelper._
import com.evolutiongaming.catshelper.{FromFuture, LogOf, ToFuture, ToTry}
import com.evolutiongaming.kafka.journal._
import com.evolutiongaming.kafka.journal.eventual._
import com.evolutiongaming.kafka.journal.util.CatsHelper._
import com.evolutiongaming.scassandra.util.FromGFuture
import com.evolutiongaming.scassandra.{CassandraClusterOf, TableName}
import com.evolutiongaming.skafka.Topic
import com.evolutiongaming.smetrics.MeasureDuration
import com.evolutiongaming.sstream.Stream


object EventualCassandra {

  def of[
    F[_]
    : Concurrent: Parallel: Timer
    : FromFuture: ToFuture: ToTry: LogOf
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
      of(config.schema, origin, metrics)
    }

    for {
      cassandraCluster <- CassandraCluster.of[F](config.client, cassandraClusterOf, config.retries)
      cassandraSession <- cassandraCluster.session
      journal          <- journal(cassandraCluster, cassandraSession).toResource
    } yield journal
  }

  def of[
    F[_]
    : Concurrent: Parallel: Timer
    : FromFuture: ToFuture: ToTry: LogOf
    : CassandraCluster: CassandraSession
    : MeasureDuration
    : JsonCodec.Decode
  ](
    schemaConfig: SchemaConfig,
    origin: Option[Origin],
    metrics: Option[EventualJournal.Metrics[F]]
  ): F[EventualJournal[F]] = {

    for {
      log          <- LogOf[F].apply(EventualCassandra.getClass)
      schema       <- SetupSchema[F](schemaConfig, origin)
      segmentNrsOf  = SegmentNrsOf[F](first = Segments.old, second = Segments.default)
      statements   <- Statements.of(schema, segmentNrsOf)
    } yield {
      val journal = apply[F](statements).withLog(log)
      metrics
        .fold(journal) { metrics => journal.withMetrics(metrics) }
        .enhanceError
    }
  }


  def apply[F[_]: Monad: Parallel](statements: Statements[F]): EventualJournal[F] = {

    new EventualJournal[F] {

      def pointer(key: Key) = {
        statements.metaJournal.journalPointer(key)
      }

      def pointers(topic: Topic) = {
        for {
          pointers <- statements.pointers(topic)
        } yield {
          TopicPointers(pointers)
        }
      }

      def read(key: Key, from: SeqNr): Stream[F, EventRecord[EventualPayloadAndType]] = {

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
          journalHead <- Stream.lift { statements.metaJournal.journalHead(key) }
          result      <- journalHead match {
            case Some(journalHead) => read(statements.records, journalHead)
            case None              => Stream.empty[F, EventRecord[EventualPayloadAndType]]
          }
        } yield result
      }
    }
  }


  final case class Statements[F[_]](
    records: JournalStatements.SelectRecords[F],
    metaJournal: MetaJournalStatements[F],
    pointers: PointerStatements.SelectAll[F])

  object Statements {

    def apply[F[_]](implicit F: Statements[F]): Statements[F] = F

    def of[F[_]: Concurrent: Parallel: CassandraSession: ToTry: JsonCodec.Decode](
      schema: Schema,
      segmentNrsOf: SegmentNrsOf[F]
    ): F[Statements[F]] = {
      val statements = (
        JournalStatements.SelectRecords.of[F](schema.journal),
        MetaJournalStatements.of(schema, segmentNrsOf),
        PointerStatements.SelectAll.of[F](schema.pointer))
      statements.parMapN(Statements[F])
    }
  }


  trait MetaJournalStatements[F[_]] {

    def journalHead(key: Key): F[Option[JournalHead]]

    def journalPointer(key: Key): F[Option[JournalPointer]]
  }

  object MetaJournalStatements {

    def of[F[_]: Concurrent: Parallel: CassandraSession](
      schema: Schema,
      segmentNrsOf: SegmentNrsOf[F]
    ): F[MetaJournalStatements[F]] = {

      val metadata = {
        val statements = (
          MetadataStatements.SelectJournalHead.of[F](schema.metadata),
          MetadataStatements.SelectJournalPointer.of[F](schema.metadata))
        statements.parMapN(apply[F])
      }

      (of[F](schema.metaJournal, segmentNrsOf), metadata).parMapN(apply[F])
    }


    def of[F[_]: Concurrent: Parallel: CassandraSession](
      metaJournal: TableName,
      segmentNrsOf: SegmentNrsOf[F]
    ): F[MetaJournalStatements[F]] = {
      val statements = (
        cassandra.MetaJournalStatements.SelectJournalHead.of[F](metaJournal),
        cassandra.MetaJournalStatements.SelectJournalPointer.of[F](metaJournal))
      statements.parMapN { (selectJournalHead, selectJournalPointer) =>
        apply(segmentNrsOf, selectJournalHead, selectJournalPointer)
      }
    }


    def apply[F[_]: Concurrent](
      metaJournal: MetaJournalStatements[F],
      metadata: MetaJournalStatements[F]
    ): MetaJournalStatements[F] = {
      new MetaJournalStatements[F] {

        def journalHead(key: Key) = {
          metaJournal
            .journalHead(key)
            .orElsePar { metadata.journalHead(key) }
        }

        def journalPointer(key: Key) = {
          metaJournal
            .journalPointer(key)
            .orElsePar { metadata.journalPointer(key) }
        }
      }
    }


    def apply[F[_]](
      journalHead: MetadataStatements.SelectJournalHead[F],
      journalPointer: MetadataStatements.SelectJournalPointer[F]
    ): MetaJournalStatements[F] = {
      val journalHead1 = journalHead
      val journalPointer1 = journalPointer
      new MetaJournalStatements[F] {

        def journalHead(key: Key) = journalHead1(key)

        def journalPointer(key: Key) = journalPointer1(key)
      }
    }


    def apply[F[_]: Concurrent](
      segmentNrsOf: SegmentNrsOf[F],
      journalHead: cassandra.MetaJournalStatements.SelectJournalHead[F],
      journalPointer: cassandra.MetaJournalStatements.SelectJournalPointer[F]
    ): MetaJournalStatements[F] = {

      val journalHead1 = journalHead
      val journalPointer1 = journalPointer

      def firstOrSecond[A](key: Key)(f: SegmentNr => F[Option[A]]): F[Option[A]] = {
        for {
          segmentNrs <- segmentNrsOf(key)
          result     <- f(segmentNrs.first).orElsePar { segmentNrs.second.flatTraverse(f) }
        } yield result
      }

      new MetaJournalStatements[F] {

        def journalHead(key: Key) = {
          firstOrSecond(key) {  segmentNr => journalHead1(key, segmentNr) }
        }

        def journalPointer(key: Key) = {
          firstOrSecond(key) {  segmentNr => journalPointer1(key, segmentNr) }
        }
      }
    }
  }
}

