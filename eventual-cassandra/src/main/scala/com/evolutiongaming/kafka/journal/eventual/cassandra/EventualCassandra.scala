package com.evolutiongaming.kafka.journal.eventual.cassandra

import com.evolutiongaming.cassandra.Session
import com.evolutiongaming.concurrent.async.Async
import com.evolutiongaming.concurrent.async.AsyncConverters._
import com.evolutiongaming.kafka.journal.FoldWhileHelper._
import com.evolutiongaming.kafka.journal.eventual._
import com.evolutiongaming.kafka.journal.AsyncHelper._
import com.evolutiongaming.kafka.journal.{Key, ReplicatedEvent, SeqNr, SeqRange}
import com.evolutiongaming.skafka.Topic

import scala.concurrent.ExecutionContext


// TODO create collection that is optimised for ordered sequence and seqNr
// TODO test EventualCassandra
object EventualCassandra {

  def apply(
    session: Session,
    config: EventualCassandraConfig)(implicit ec: ExecutionContext): EventualJournal = {

    val statements = for {
      tables <- CreateSchema(config.schema, session)
      prepareAndExecute = PrepareAndExecute(session, config.retries)
      statements <- Statements(tables, prepareAndExecute)
    } yield {
      statements
    }

    apply(statements)
  }

  def apply(statements: Async[Statements]): EventualJournal = new EventualJournal {

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
        metadata <- statements.selectMetadata(key)
        result <- metadata.fold(s.continue.async) { metadata =>
          read(statements.selectRecords, metadata)
        }
      } yield {
        result
      }
    }


    def lastSeqNr(key: Key, from: SeqNr) = {

      def lastSeqNr(statements: Statements, metadata: Option[Metadata]) = {
        metadata.fold(Option.empty[SeqNr].async) { metadata =>
          LastSeqNr(key, from, metadata, statements.selectLastRecord)
        }
      }

      for {
        statements <- statements
        metadata <- statements.selectMetadata(key)
        seqNr <- lastSeqNr(statements, metadata)
      } yield {
        seqNr
      }
    }
  }


  final case class Statements(
    selectLastRecord: JournalStatement.SelectLastRecord.Type,
    selectRecords: JournalStatement.SelectRecords.Type,
    selectMetadata: MetadataStatement.Select.Type,
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
          selectLastRecord = selectLastRecord,
          selectRecords = selectRecords,
          selectMetadata = selectMetadata,
          selectPointers = selectPointers)
      }
    }
  }
}

