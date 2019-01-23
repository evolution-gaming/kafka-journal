package com.evolutiongaming.kafka.journal.eventual.cassandra

import cats.Monad
import cats.effect.{Clock, Concurrent, Resource}
import cats.implicits._
import com.evolutiongaming.kafka.journal._
import com.evolutiongaming.kafka.journal.eventual._
import com.evolutiongaming.kafka.journal.util.{FromFuture, Par, ToFuture}
import com.evolutiongaming.skafka.Topic


// TODO test EventualCassandra
object EventualCassandra {

  def of[F[_] : Concurrent : Par : Clock : FromFuture : ToFuture : LogOf](
    config: EventualCassandraConfig,
    metrics: Option[EventualJournal.Metrics[F]]): Resource[F, EventualJournal[F]] = {

    def journal(implicit cassandraSession: CassandraSession[F]) = {
      implicit val cassandraSync = CassandraSync[F](config.schema)
      of(config.schema, metrics)
    }

    for {
      cassandraCluster <- CassandraCluster.of[F](config.client, config.retries)
      cassandraSession <- cassandraCluster.session
      journal          <- Resource.liftF(journal(cassandraSession))
    } yield journal
  }

  def of[F[_] : Monad : Par : CassandraSession : CassandraSync : LogOf : Clock](
    schemaConfig: SchemaConfig,
    metrics: Option[EventualJournal.Metrics[F]]): F[EventualJournal[F]] = {

    for {
      log        <- LogOf[F].apply(EventualCassandra.getClass)
      tables     <- CreateSchema[F](schemaConfig)
      statements <- Statements.of[F](tables)
    } yield {
      val journal = apply[F](statements, log)
      val withLog = journal.withLog(log)
      metrics.fold(withLog) { metrics => withLog.withMetrics(metrics) }
    }
  }


  def apply[F[_] : Monad : Par](statements: Statements[F], log: Log[F]): EventualJournal[F] = {

    new EventualJournal[F] {

      def pointers(topic: Topic) = {
        statements.pointers(topic)
      }

      def read(key: Key, from: SeqNr): Stream[F, ReplicatedEvent] = {

        def read(statement: JournalStatement.SelectRecords[F], metadata: Metadata) = {

          def read(from: SeqNr) = new Stream[F, ReplicatedEvent] {

            def foldWhileM[L, R](l: L)(f: (L, ReplicatedEvent) => F[Either[L, R]]) = {

              case class S(l: L, seqNr: SeqNr)

              val ff = (s: S, replicated: ReplicatedEvent) => {
                for {
                  result <- f(s.l, replicated)
                } yield {
                  result.leftMap { l => S(l, replicated.event.seqNr) }
                }
              }

              val segment = Segment(from, metadata.segmentSize)

              (from, segment, l).tailRecM { case (from, segment, l) =>
                val range = SeqRange(from, SeqNr.Max) // TODO do we need range here ?
                for {
                  result <- statement(key, segment.nr, range).foldWhileM[S, R](S(l, from))(ff) // TODO
                } yield result match {
                  case Left(s) =>
                    val result = for {
                      from <- s.seqNr.next
                      segment <- segment.next(from)
                    } yield {
                      (from, segment, s.l).asLeft[Either[L, R]]
                    }
                    result getOrElse s.l.asLeft[R].asRight[(SeqNr, Segment, L)]

                  case Right(r) => r.asRight[L].asRight[(SeqNr, Segment, L)]
                }
              }
            }
          }

          metadata.deleteTo match {
            case None           => read(from)
            case Some(deleteTo) =>
              if (from > deleteTo) read(from)
              else deleteTo.next match {
                case Some(from) => read(from)
                case None       => Stream.empty[F, ReplicatedEvent]
              }
          }
        }

        for {
          metadata <- Stream.lift(statements.metadata(key))
          result   <- metadata.fold(Stream.empty[F, ReplicatedEvent]) { metadata =>
            read(statements.records, metadata)
          }
        } yield {
          result
        }
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
    pointers: PointerStatement.SelectPointers[F])

  object Statements {

    def apply[F[_]](implicit F: Statements[F]): Statements[F] = F

    def of[F[_] : Par : Monad : CassandraSession](tables: Tables): F[Statements[F]] = {
      val statements = (
        JournalStatement.SelectRecords.of[F](tables.journal),
        MetadataStatement.Select.of[F](tables.metadata),
        PointerStatement.SelectPointers.of[F](tables.pointer))
      Par[F].mapN(statements)(Statements[F])
    }
  }
}

