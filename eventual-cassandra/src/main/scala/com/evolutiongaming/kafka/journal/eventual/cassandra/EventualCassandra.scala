package com.evolutiongaming.kafka.journal.eventual.cassandra

import com.datastax.driver.core.policies.LoggingRetryPolicy
import com.datastax.driver.core.{Metadata => _, _}
import com.evolutiongaming.cassandra.NextHostRetryPolicy
import com.evolutiongaming.kafka.journal.Alias._
import com.evolutiongaming.kafka.journal.FoldWhileHelper._
import com.evolutiongaming.kafka.journal.FutureHelper._
import com.evolutiongaming.kafka.journal.eventual._
import com.evolutiongaming.kafka.journal.{ReplicatedEvent, SeqRange}
import com.evolutiongaming.safeakka.actor.ActorLog
import com.evolutiongaming.skafka.Topic

import scala.concurrent.{ExecutionContext, Future}


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


      def foldWhile[S](id: Id, from: SeqNr, s: S)(f: Fold[S, ReplicatedEvent]) = {

        def foldWhile(statement: JournalStatement.SelectRecords.Type, metadata: Metadata) = {

          def foldWhile(from: SeqNr, segment: Segment, s: S): Future[(S, Continue)] = {
            val range = SeqRange(from, SeqNr.Max) // TODO do we need range here ?
            for {
              result <- statement(id, segment.nr, range, (s, from)) { case ((s, _), replicated) =>
                val (ss, continue) = f(s, replicated)
                ((ss, replicated.event.seqNr), continue)
              }
              result <- {
                val ((s, seqNr), continue) = result
                if (continue) {
                  val from = seqNr.next
                  segment.next(from).fold((s, continue).future) { segment =>
                    foldWhile(from, segment, s)
                  }
                } else {
                  (s, continue).future
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
          metadata <- statements.selectMetadata(id)
          result <- metadata.fold((s, true).future) { metadata =>
            foldWhile(statements.selectRecords, metadata)
          }
        } yield {
          result
        }
      }


      def lastSeqNr(id: Id, from: SeqNr) = {

        def lastSeqNr(statements: Statements, metadata: Option[Metadata]) = {
          metadata.fold(from.future) { metadata =>
            LastSeqNr(id, from, statements.selectLastRecord, metadata) // TODO remove this, use lastSeqNr from metadata
          }
        }

        for {
          statements <- statements
          metadata <- statements.selectMetadata(id)
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
    selectSegmentSize: MetadataStatement.SelectSegmentSize.Type,
    updatePointer: PointerStatement.Update.Type,
    selectPointer: PointerStatement.Select.Type,
    selectTopicPointer: PointerStatement.SelectTopicPointers.Type)

  object Statements {

    def apply(tables: Tables, prepareAndExecute: PrepareAndExecute)(implicit ec: ExecutionContext): Future[Statements] = {

      val selectLastRecord = JournalStatement.SelectLastRecord(tables.journal, prepareAndExecute)
      val listRecords = JournalStatement.SelectRecords(tables.journal, prepareAndExecute)
      val selectMetadata = MetadataStatement.Select(tables.metadata, prepareAndExecute)
      val selectSegmentSize = MetadataStatement.SelectSegmentSize(tables.metadata, prepareAndExecute)
      val updatePointer = PointerStatement.Update(tables.pointer, prepareAndExecute)
      val selectPointer = PointerStatement.Select(tables.pointer, prepareAndExecute)
      val selectTopicPointers = PointerStatement.SelectTopicPointers(tables.pointer, prepareAndExecute)

      for {
        selectLastRecord <- selectLastRecord
        listRecords <- listRecords
        selectMetadata <- selectMetadata
        selectSegmentSize <- selectSegmentSize
        updatePointer <- updatePointer
        selectPointer <- selectPointer
        selectTopicPointers <- selectTopicPointers
      } yield {
        Statements(
          selectLastRecord,
          listRecords,
          selectMetadata,
          selectSegmentSize,
          updatePointer,
          selectPointer,
          selectTopicPointers)
      }
    }
  }
}

