package com.evolutiongaming.kafka.journal.eventual.cassandra

import cats.effect.{Concurrent, Resource, Timer}
import cats.syntax.all._
import cats.{Monad, Parallel}
import com.evolutiongaming.catshelper.CatsHelper._
import com.evolutiongaming.catshelper.{LogOf, ToTry}
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
    : ToTry: LogOf
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
      segmentNrsOf  = SegmentNrsOf[F](first = Segments.default, second = Segments.old)
      statements   <- Statements.of(schema, segmentNrsOf, Segments.default)
    } yield {
      val journal = apply[F](statements).withLog(log)
      metrics
        .fold(journal) { metrics => journal.withMetrics(metrics) }
        .enhanceError
    }
  }


  private sealed abstract class Main

  def apply[F[_]: Monad](statements: Statements[F]): EventualJournal[F] = {

    new Main with EventualJournal[F] {

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

      def ids(topic: Topic) = {
        statements.metaJournal.ids(topic)
      }
    }
  }


  final case class Statements[F[_]](
    records: JournalStatements.SelectRecords[F],
    metaJournal: MetaJournalStatements[F],
    pointers: PointerStatements.SelectAll[F])

  object Statements {

    def apply[F[_]](implicit F: Statements[F]): Statements[F] = F

    def of[F[_]: Concurrent: CassandraSession: ToTry: JsonCodec.Decode](
      schema: Schema,
      segmentNrsOf: SegmentNrsOf[F],
      segments: Segments
    ): F[Statements[F]] = {
      for {
        selectRecords  <- JournalStatements.SelectRecords.of[F](schema.journal)
        metaJournal    <- MetaJournalStatements.of(schema, segmentNrsOf, segments)
        selectPointers <- PointerStatements.SelectAll.of[F](schema.pointer)
      } yield {
        Statements(selectRecords, metaJournal, selectPointers)
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
      segments: Segments
    ): F[MetaJournalStatements[F]] = {
      of(schema.metaJournal, segmentNrsOf, segments)
    }

    def of[F[_]: Concurrent: CassandraSession](
      metaJournal: TableName,
      segmentNrsOf: SegmentNrsOf[F],
      segments: Segments,
    ): F[MetaJournalStatements[F]] = {
      for {
        selectJournalHead    <- cassandra.MetaJournalStatements.SelectJournalHead.of[F](metaJournal)
        selectJournalPointer <- cassandra.MetaJournalStatements.SelectJournalPointer.of[F](metaJournal)
        selectIds            <- cassandra.MetaJournalStatements.SelectIds.of[F](metaJournal)
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
            segmentNr <- Stream.from(segments.segmentNrs)
            id        <- ids1(topic, segmentNr)
          } yield id
        }
      }
    }
  }
}

