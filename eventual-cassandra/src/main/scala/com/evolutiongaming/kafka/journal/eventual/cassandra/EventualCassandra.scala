package com.evolutiongaming.kafka.journal.eventual.cassandra

import cats.{Monad, Parallel}
import cats.effect.{Concurrent, Resource, Timer}
import cats.implicits._
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
      val journal = apply[F](statements)
      val withLog = journal.withLog(log)
      metrics.fold(withLog) { metrics => withLog.withMetrics(metrics) }
    }
  }


  def apply[F[_] : Monad : Parallel](statements: Statements[F]): EventualJournal[F] = {

    new EventualJournal[F] {

      def pointers(topic: Topic) = {
        for {
          pointers <- statements.pointers(topic)
        } yield {
          TopicPointers(pointers)
        }
      }

      def read(key: Key, from: SeqNr): Stream[F, EventRecord] = {

        def read(statement: JournalStatement.SelectRecords[F], metadata: Head) = {

          def read(from: SeqNr) = new Stream[F, EventRecord] {

            def foldWhileM[L, R](l: L)(f: (L, EventRecord) => F[Either[L, R]]) = {

              case class S(l: L, seqNr: SeqNr)

              val ff = (s: S, record: EventRecord) => {
                for {
                  result <- f(s.l, record)
                } yield {
                  result.leftMap { l => S(l, record.event.seqNr) }
                }
              }

              val segment = Segment.unsafe(from, metadata.segmentSize)

              (from, segment, l).tailRecM { case (from, segment, l) =>
                val range = SeqRange(from, SeqNr.max) // TODO do we need range here ?
                for {
                  result <- statement(key, segment.nr, range).foldWhileM[S, R](S(l, from))(ff) // TODO
                } yield result match {
                  case r: Right[S, R] => r.leftCast[L].asRight[(SeqNr, Segment, L)]
                  case Left(s)        =>
                    val result = for {
                      from    <- s.seqNr.next[Option]
                      segment <- segment.nextUnsafe(from)
                    } yield {
                      (from, segment, s.l).asLeft[Either[L, R]]
                    }
                    result getOrElse s.l.asLeft[R].asRight[(SeqNr, Segment, L)]
                }
              }
            }
          }

          metadata.deleteTo match {
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
          metadata <- Stream.lift(statements.metadata(key))
          result   <- metadata.fold(Stream.empty[F, EventRecord]) { head =>
            read(statements.records, head)
          }
        } yield result
      }

      def pointer(key: Key) = {
        for {
          metadata <- statements.metadata(key)
        } yield for {
          metadata <- metadata
        } yield {
          Pointer(metadata.partitionOffset, metadata.seqNr)
        }
      }
    }
  }


  final case class Statements[F[_]](
    records: JournalStatement.SelectRecords[F],
    metadata: MetadataStatement.Select[F],
    pointers: PointerStatement.SelectAll[F])

  object Statements {

    def apply[F[_]](implicit F: Statements[F]): Statements[F] = F

    def of[F[_] : Parallel : Monad : CassandraSession](schema: Schema): F[Statements[F]] = {
      val statements = (
        JournalStatement.SelectRecords.of[F](schema.journal),
        MetadataStatement.Select.of[F](schema.metadata),
        PointerStatement.SelectAll.of[F](schema.pointer))
      statements.parMapN(Statements[F])
    }
  }
}

