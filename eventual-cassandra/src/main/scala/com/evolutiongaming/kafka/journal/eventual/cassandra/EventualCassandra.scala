package com.evolutiongaming.kafka.journal.eventual.cassandra

import cats.effect.{Concurrent, Resource, Timer}
import cats.implicits._
import cats.{Monad, Parallel}
import com.evolutiongaming.catshelper.{FromFuture, LogOf, ToFuture}
import com.evolutiongaming.kafka.journal._
import com.evolutiongaming.kafka.journal.eventual._
import com.evolutiongaming.kafka.journal.util.OptionHelper._
import com.evolutiongaming.scassandra.CassandraClusterOf
import com.evolutiongaming.scassandra.util.FromGFuture
import com.evolutiongaming.skafka.Topic
import com.evolutiongaming.smetrics.MeasureDuration
import com.evolutiongaming.sstream.Stream


object EventualCassandra {

  def of[F[_] : Concurrent : Parallel : Timer : FromFuture : ToFuture : LogOf : FromGFuture : MeasureDuration](
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
      journal          <- Resource.liftF(journal(cassandraCluster, cassandraSession))
    } yield journal
  }

  def of[F[_] : Concurrent : Parallel : CassandraCluster : CassandraSession : LogOf : Timer : FromFuture : ToFuture : MeasureDuration](
    schemaConfig: SchemaConfig,
    origin: Option[Origin],
    metrics: Option[EventualJournal.Metrics[F]]
  ): F[EventualJournal[F]] = {

    for {
      log        <- LogOf[F].apply(EventualCassandra.getClass)
      schema     <- SetupSchema[F](schemaConfig, origin)
      statements <- Statements.of[F](schema)
    } yield {
      val journal = apply[F](statements).withLog(log)
      metrics
        .fold(journal) { metrics => journal.withMetrics(metrics) }
        .enhanceError
    }
  }


  def apply[F[_] : Monad : Parallel](statements: Statements[F]): EventualJournal[F] = {

    new EventualJournal[F] {

      def pointer(key: Key) = {
        statements.pointer(key)
      }

      def pointers(topic: Topic) = {
        for {
          pointers <- statements.pointers(topic)
        } yield {
          TopicPointers(pointers)
        }
      }

      def read(key: Key, from: SeqNr): Stream[F, EventRecord] = {

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
              if (from > deleteTo) read(from)
              else deleteTo.next[Option] match {
                case Some(from) => read(from)
                case None       => Stream.empty[F, EventRecord]
              }
          }
        }

        for {
          head   <- Stream.lift(statements.metadata(key))
          result <- head.fold(Stream.empty[F, EventRecord]) { head => read(statements.records, head) }
        } yield result
      }
    }
  }


  final case class Statements[F[_]](
    records: JournalStatements.SelectRecords[F],
    metadata: MetadataStatements.SelectHead[F],
    pointer: MetadataStatements.SelectJournalPointer[F],
    pointers: PointerStatements.SelectAll[F])

  object Statements {

    def apply[F[_]](implicit F: Statements[F]): Statements[F] = F

    def of[F[_] : Parallel : Monad : CassandraSession](schema: Schema): F[Statements[F]] = {
      val statements = (
        JournalStatements.SelectRecords.of[F](schema.journal),
        MetadataStatements.SelectHead.of[F](schema.metadata),
        MetadataStatements.SelectJournalPointer.of[F](schema.metadata),
        PointerStatements.SelectAll.of[F](schema.pointer))
      statements.parMapN(Statements[F])
    }
  }
}

