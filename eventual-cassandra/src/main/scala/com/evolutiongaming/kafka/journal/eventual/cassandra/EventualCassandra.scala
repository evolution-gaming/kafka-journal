package com.evolutiongaming.kafka.journal.eventual.cassandra

import cats.FlatMap
import com.evolutiongaming.concurrent.async.Async
import com.evolutiongaming.concurrent.async.AsyncConverters._
import com.evolutiongaming.kafka.journal.FoldWhile._
import com.evolutiongaming.kafka.journal._
import com.evolutiongaming.kafka.journal.IO2.ops._
import com.evolutiongaming.kafka.journal.eventual._
import com.evolutiongaming.scassandra.Session
import com.evolutiongaming.skafka.Topic

import scala.concurrent.ExecutionContext


// TODO create collection that is optimised for ordered sequence and seqNr
// TODO test EventualCassandra
object EventualCassandra {

  def apply(
    config: EventualCassandraConfig,
    log: Log[Async],
    origin: Option[Origin])(implicit
    ec: ExecutionContext,
    session: Session): EventualJournal[Async] = {

    import com.evolutiongaming.kafka.journal.AsyncImplicits._

    implicit val cassandraSession = CassandraSession(CassandraSession[Async](session), config.retries)
    implicit val cassandraSync = CassandraSync(config.schema, origin)
    val statements = for {
      tables     <- CreateSchema(config.schema)
      statements <- Statements(tables)
    } yield {
      statements
    }

    apply(statements, log)
  }

  def apply(statements: Async[Statements[Async]], log: Log[Async]): EventualJournal[Async] = new EventualJournal[Async] {

    def pointers(topic: Topic) = {
      for {
        statements <- statements
        pointers   <- statements.pointers(topic)
      } yield {
        pointers
      }
    }

    def read[S](key: Key, from: SeqNr, s: S)(f: Fold[S, ReplicatedEvent]) = {

      def read(statement: JournalStatement.SelectRecords.Type[Async], metadata: Metadata) = {

        case class SS(seqNr: SeqNr, s: S)

        val ff = (ss: SS, replicated: ReplicatedEvent) => {
          for {
            s <- f(ss.s, replicated)
          } yield SS(replicated.event.seqNr, s)
        }

        def read(from: SeqNr) = {

          def read(from: SeqNr, segment: Segment, s: S): Async[Switch[S]] = {
            val range = SeqRange(from, SeqNr.Max) // TODO do we need range here ?

            for {
              result <- statement(key, segment.nr, range, SS(from, s))(ff)
              result <- {
                val ss = result.s
                val s = ss.s
                val seqNr = ss.seqNr
                if (result.stop) s.stop.async
                else {
                  val result = for {
                    from <- seqNr.next
                    segment <- segment.next(from)
                  } yield {
                    read(from, segment, s)
                  }
                  result getOrElse s.continue.async
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
              case None       => s.continue.async
            }
        }
      }

      for {
        statements <- statements
        metadata <- statements.metadata(key)
        result <- metadata.fold(s.continue.async) { metadata =>
          read(statements.records, metadata)
        }
      } yield {
        result
      }
    }

    def pointer(key: Key) = {
      for {
        statements <- statements
        metadata <- statements.metadata(key)
      } yield for {
        metadata <- metadata
      } yield {
        Pointer(metadata.partitionOffset, metadata.seqNr)
      }
    }
  }


  final case class Statements[F[_]](
    records: JournalStatement.SelectRecords.Type[F],
    metadata: MetadataStatement.Select.Type[F],
    pointers: PointerStatement.SelectPointers.Type[F])

  object Statements {

    def apply[F[_]: IO2/*TODO*/ : FromFuture/*TODO*/ : FlatMap : CassandraSession](tables: Tables): F[Statements[F]] = {

      // TODO run in parallel with IO
      val records  = JournalStatement.SelectRecords(tables.journal)
      val metadata = MetadataStatement.Select(tables.metadata)
      val pointers = PointerStatement.SelectPointers(tables.pointer)

      for {
        records  <- records
        metadata <- metadata
        pointers <- pointers
      } yield {
        Statements(
          records  = records,
          metadata = metadata,
          pointers = pointers)
      }
    }
  }
}

