package com.evolutiongaming.kafka.journal.eventual.cassandra

import com.datastax.driver.core.ConsistencyLevel
import com.datastax.driver.core.policies.LoggingRetryPolicy
import com.evolutiongaming.cassandra.{NextHostRetryPolicy, Session}
import com.evolutiongaming.concurrent.async.Async
import com.evolutiongaming.concurrent.async.AsyncConverters._
import com.evolutiongaming.kafka.journal.Alias._
import com.evolutiongaming.kafka.journal.FoldWhileHelper._
import com.evolutiongaming.kafka.journal.eventual._
import com.evolutiongaming.kafka.journal.{Key, ReplicatedEvent, SeqRange}
import com.evolutiongaming.safeakka.actor.ActorLog
import com.evolutiongaming.skafka.Topic

import scala.concurrent.ExecutionContext


// TODO create collection that is optimised for ordered sequence and seqNr
object EventualCassandra {

  def apply(
    session: Session,
    schemaConfig: SchemaConfig,
    config: EventualCassandraConfig,
    log: ActorLog)(implicit ec: ExecutionContext): EventualJournal = {

    val retries = 3

    val statementConfig = StatementConfig(
      idempotent = true, /*TODO remove from here*/
      consistencyLevel = ConsistencyLevel.ONE,
      retryPolicy = new LoggingRetryPolicy(NextHostRetryPolicy(retries)))


    val statements = for {
      tables <- CreateSchema(schemaConfig, session)
      prepareAndExecute = PrepareAndExecute(session, statementConfig)
      statements <- Statements(tables, prepareAndExecute)
    } yield {
      statements
    }

    new EventualJournal {

      def topicPointers(topic: Topic) = {
        for {
          statements <- statements
          topicPointers <- statements.selectTopicPointer(topic)
        } yield {
          topicPointers
        }
      }


      def foldWhile[S](key: Key, from: SeqNr, s: S)(f: Fold[S, ReplicatedEvent]) = {

        def foldWhile(statement: JournalStatement.SelectRecords.Type, metadata: Metadata) = {

          def foldWhile(from: SeqNr, segment: Segment, s: S): Async[Switch[S]] = {
            val range = SeqRange(from, SeqNr.Max) // TODO do we need range here ?
            for {
              result <- statement(key, segment.nr, range, (s, from)) { case ((s, _), replicated) =>
                val switch = f(s, replicated)
                for {s <- switch} yield (s, replicated.event.seqNr)
              }
              result <- {
                val (s, seqNr) = result.s
                if (result.stop) s.stop.async
                else {
                  val from = seqNr.next
                  segment.next(from).fold(s.continue.async) { segment =>
                    foldWhile(from, segment, s)
                  }
                }
              }
            } yield result
          }

          val fromFixed = from max metadata.deleteTo.next
          val segment = Segment(fromFixed, metadata.segmentSize)
          foldWhile(fromFixed, segment, s)
        }

        for {
          statements <- statements
          metadata <- statements.selectMetadata(key)
          result <- metadata.fold(s.continue.async) { metadata =>
            foldWhile(statements.selectRecords, metadata)
          }
        } yield {
          result
        }
      }


      def lastSeqNr(key: Key, from: SeqNr) = {

        def lastSeqNr(statements: Statements, metadata: Option[Metadata]) = {
          metadata.fold(from.async) { metadata =>
            LastSeqNr(key, from, statements.selectLastRecord, metadata) // TODO remove this, use lastSeqNr from metadata
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
  }


  final case class Statements(
    selectLastRecord: JournalStatement.SelectLastRecord.Type,
    selectRecords: JournalStatement.SelectRecords.Type,
    selectMetadata: MetadataStatement.Select.Type,
    updatePointer: PointerStatement.Update.Type,
    selectPointer: PointerStatement.Select.Type,
    selectTopicPointer: PointerStatement.SelectTopicPointers.Type)

  object Statements {

    def apply(tables: Tables, prepareAndExecute: PrepareAndExecute)(implicit ec: ExecutionContext): Async[Statements] = {

      val selectLastRecord = JournalStatement.SelectLastRecord(tables.journal, prepareAndExecute)
      val listRecords = JournalStatement.SelectRecords(tables.journal, prepareAndExecute)
      val selectMetadata = MetadataStatement.Select(tables.metadata, prepareAndExecute)
      val updatePointer = PointerStatement.Update(tables.pointer, prepareAndExecute)
      val selectPointer = PointerStatement.Select(tables.pointer, prepareAndExecute)
      val selectTopicPointers = PointerStatement.SelectTopicPointers(tables.pointer, prepareAndExecute)

      for {
        selectLastRecord <- selectLastRecord
        listRecords <- listRecords
        selectMetadata <- selectMetadata
        updatePointer <- updatePointer
        selectPointer <- selectPointer
        selectTopicPointers <- selectTopicPointers
      } yield {
        Statements(
          selectLastRecord,
          listRecords,
          selectMetadata,
          updatePointer,
          selectPointer,
          selectTopicPointers)
      }
    }
  }
}

