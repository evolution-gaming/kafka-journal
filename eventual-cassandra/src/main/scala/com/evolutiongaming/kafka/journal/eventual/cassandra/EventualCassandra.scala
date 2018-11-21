package com.evolutiongaming.kafka.journal.eventual.cassandra

import com.evolutiongaming.concurrent.async.Async
import com.evolutiongaming.concurrent.async.AsyncConverters._
import com.evolutiongaming.kafka.journal.AsyncImplicits._
import com.evolutiongaming.kafka.journal.FoldWhile._
import com.evolutiongaming.kafka.journal._
import com.evolutiongaming.kafka.journal.IO.ops._
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
    session: Session): EventualJournal = {

    implicit val cassandraSession = CassandraSession[Async](session, config.retries)
    val cassandraSync = CassandraSync(config.schema, origin)
    val statements = for {
      tables <- CreateSchema(config.schema, cassandraSync)
      statements <- Statements(tables, cassandraSession)
    } yield {
      statements
    }

    apply(statements, log)
  }

  def apply(statements: Async[Statements[Async]], log: Log[Async]): EventualJournal = new EventualJournal {

    def pointers(topic: Topic) = {
      for {
        statements <- statements
        topicPointers <- statements.selectPointers(topic)
      } yield {
        topicPointers
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

    def pointer(key: Key, from: SeqNr) = {
      for {
        statements <- statements
        metadata <- statements.metadata(key)
      } yield for {
        metadata <- metadata
        seqNr = metadata.seqNr
        if seqNr >= from
      } yield {
        Pointer(metadata.partitionOffset, seqNr)
      }
    }
  }


  final case class Statements[F[_]](
    lastRecord: JournalStatement.SelectLastRecord.Type[F],
    records: JournalStatement.SelectRecords.Type[F],
    metadata: MetadataStatement.Select.Type[F],
    selectPointers: PointerStatement.SelectPointers.Type[F])

  object Statements {

    def apply[F[_]: IO: FromFuture](tables: Tables, session: CassandraSession[F]): F[Statements[F]] = {

      val selectLastRecord = JournalStatement.SelectLastRecord(tables.journal, session)
      val selectRecords = JournalStatement.SelectRecords(tables.journal, session)
      val selectMetadata = MetadataStatement.Select(tables.metadata, session)
      val selectPointers = PointerStatement.SelectPointers(tables.pointer, session)

      for {
        selectLastRecord <- selectLastRecord
        selectRecords <- selectRecords
        selectMetadata <- selectMetadata
        selectPointers <- selectPointers
      } yield {
        Statements(
          lastRecord = selectLastRecord,
          records = selectRecords,
          metadata = selectMetadata,
          selectPointers = selectPointers)
      }
    }
  }
}

