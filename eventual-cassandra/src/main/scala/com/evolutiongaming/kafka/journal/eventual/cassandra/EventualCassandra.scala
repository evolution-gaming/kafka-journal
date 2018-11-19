package com.evolutiongaming.kafka.journal.eventual.cassandra

import com.evolutiongaming.concurrent.async.Async
import com.evolutiongaming.concurrent.async.AsyncConverters._
import com.evolutiongaming.kafka.journal.FoldWhile._
import com.evolutiongaming.kafka.journal._
import com.evolutiongaming.kafka.journal.eventual._
import com.evolutiongaming.scassandra.Session
import com.evolutiongaming.skafka.Topic

import scala.concurrent.ExecutionContext


// TODO create collection that is optimised for ordered sequence and seqNr
// TODO test EventualCassandra
object EventualCassandra {

  def apply(
    session: Session,
    config: EventualCassandraConfig,
    log: Log[Async])(implicit ec: ExecutionContext): EventualJournal = {

    val statements = for {
      tables <- CreateSchema(config.schema)(ec, session)
      prepareAndExecute = PrepareAndExecute(session, config.retries)
      statements <- Statements(tables, prepareAndExecute)
    } yield {
      statements
    }

    apply(statements, log)
  }

  def apply(statements: Async[Statements], log: Log[Async]): EventualJournal = new EventualJournal {

    def pointers(topic: Topic) = {
      for {
        statements <- statements
        topicPointers <- statements.selectPointers(topic)
      } yield {
        topicPointers
      }
    }

    def read[S](key: Key, from: SeqNr, s: S)(f: Fold[S, ReplicatedEvent]) = {

      def read(statement: JournalStatement.SelectRecords.Type, metadata: Metadata) = {

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


  final case class Statements(
    lastRecord: JournalStatement.SelectLastRecord.Type[Async],
    records: JournalStatement.SelectRecords.Type,
    metadata: MetadataStatement.Select.Type,
    selectPointers: PointerStatement.SelectPointers.Type)

  object Statements {

    def apply(tables: Tables, prepareAndExecute: PrepareAndExecute)(implicit ec: ExecutionContext): Async[Statements] = {

      val selectLastRecord = JournalStatement.SelectLastRecord(tables.journal, prepareAndExecute)
      val selectRecords = JournalStatement.SelectRecords(tables.journal, prepareAndExecute)
      val selectMetadata = MetadataStatement.Select(tables.metadata, prepareAndExecute)
      val selectPointers = PointerStatement.SelectPointers(tables.pointer, prepareAndExecute)

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

