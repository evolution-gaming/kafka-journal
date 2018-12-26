package com.evolutiongaming.kafka.journal.eventual.cassandra

import cats.implicits._
import cats.effect.IO
import cats.{Monad}
import com.evolutiongaming.concurrent.async.Async
import com.evolutiongaming.concurrent.async.AsyncConverters._
import com.evolutiongaming.kafka.journal.FoldWhile._
import com.evolutiongaming.kafka.journal._
import com.evolutiongaming.kafka.journal.eventual._
import com.evolutiongaming.kafka.journal.util.{FromFuture, Par}
import com.evolutiongaming.safeakka.actor.ActorLog
import com.evolutiongaming.scassandra.Session
import com.evolutiongaming.skafka.Topic

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._


// TODO test EventualCassandra
object EventualCassandra {

  def apply(
    config: EventualCassandraConfig,
    actorLog: ActorLog,
    origin: Option[Origin])(implicit
    ec: ExecutionContext,
    session: Session): EventualJournal[Async] = {

    async(config, actorLog, origin).get(30.seconds) // TODO
  }


  def async(
    config: EventualCassandraConfig,
    actorLog: ActorLog,
    origin: Option[Origin])(implicit
    ec: ExecutionContext,
    session: Session): Async[EventualJournal[Async]] = {

    implicit val cs = IO.contextShift(ec)
    implicit val fromFuture = FromFuture.lift[IO]
    implicit val cassandraSession = CassandraSession(CassandraSession[IO](session), config.retries)
    implicit val cassandraSync = CassandraSync[IO](config.schema, origin)
    implicit val log = Log.fromLog[IO](actorLog)

    val journal = for {
      journal <- of[IO](config)
    } yield {
      new EventualJournal[Async] {
        def pointers(topic: Topic) = {
          journal.pointers(topic).unsafeToFuture().async
        }

        def read[S](key: Key, from: SeqNr, s: S)(f: Fold[S, ReplicatedEvent]) = {
          journal.read(key, from, s)(f).unsafeToFuture().async
        }

        def pointer(key: Key) = {
          journal.pointer(key).unsafeToFuture().async
        }
      }
    }
    Async(journal.unsafeToFuture())
  }


  def of[F[_] : Monad : Par : CassandraSession : CassandraSync : Log](config: EventualCassandraConfig): F[EventualJournal[F]] = {
    for {
      tables     <- CreateSchema[F](config.schema)
      statements <- Statements.of[F](tables)
    } yield {
      implicit val statements1 = statements
      apply[F]
    }
  }

  
  def apply[F[_] : Monad : Par : Statements : Log]: EventualJournal[F] = {

    new EventualJournal[F] {

      def pointers(topic: Topic) = {
        Statements[F].pointers(topic)
      }

      def read[S](key: Key, from: SeqNr, s: S)(f: Fold[S, ReplicatedEvent]) = {

        def read(statement: JournalStatement.SelectRecords[F], metadata: Metadata) = {

          case class SS(seqNr: SeqNr, s: S)

          val ff = (ss: SS, replicated: ReplicatedEvent) => {
            for {
              s <- f(ss.s, replicated)
            } yield SS(replicated.event.seqNr, s)
          }

          def read(from: SeqNr) = {

            def read(from: SeqNr, segment: Segment, s: S): F[Switch[S]] = {
              val range = SeqRange(from, SeqNr.Max) // TODO do we need range here ?

              for {
                result <- statement(key, segment.nr, range, SS(from, s))(ff)
                result <- {
                  val ss = result.s
                  val s = ss.s
                  val seqNr = ss.seqNr
                  if (result.stop) s.stop.pure[F]
                  else {
                    val result = for {
                      from    <- seqNr.next
                      segment <- segment.next(from)
                    } yield {
                      read(from, segment, s)
                    }
                    result getOrElse s.continue.pure[F]
                  }
                }
              } yield result
            }

            val segment = Segment(from, metadata.segmentSize)
            read(from, segment, s)
          }

          metadata.deleteTo match {
            case None           => read(from)
            case Some(deleteTo) =>
              if (from > deleteTo) read(from)
              else deleteTo.next match {
                case Some(from) => read(from)
                case None       => s.continue.pure[F]
              }
          }
        }

        for {
          metadata <- Statements[F].metadata(key)
          result   <- metadata.fold(s.continue.pure[F]) { metadata =>
            read(Statements[F].records, metadata)
          }
        } yield {
          result
        }
      }

      def pointer(key: Key) = {
        for {
          metadata <- Statements[F].metadata(key)
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

