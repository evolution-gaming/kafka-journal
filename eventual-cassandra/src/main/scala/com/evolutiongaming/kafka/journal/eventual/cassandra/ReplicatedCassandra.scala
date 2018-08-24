package com.evolutiongaming.kafka.journal.eventual.cassandra

import java.time.Instant

import akka.actor.ActorSystem
import com.datastax.driver.core.ConsistencyLevel
import com.datastax.driver.core.policies.LoggingRetryPolicy
import com.evolutiongaming.cassandra.{NextHostRetryPolicy, Session}
import com.evolutiongaming.concurrent.async.Async
import com.evolutiongaming.kafka.journal.eventual._
import com.evolutiongaming.kafka.journal.{Key, ReplicatedEvent, SeqNr}
import com.evolutiongaming.nel.Nel
import com.evolutiongaming.safeakka.actor.ActorLog
import com.evolutiongaming.skafka.Topic

import scala.annotation.tailrec
import scala.concurrent.ExecutionContext


// TODO create collection that is optimised for ordered sequence and seqNr
// TODO redesign EventualDbCassandra so it can hold stat and called recursively
// TODO test ReplicatedCassandra
// TODO add logs to ReplicatedCassandra
object ReplicatedCassandra {

  def apply(
    session: Session,
    config: EventualCassandraConfig)(implicit system: ActorSystem, ec: ExecutionContext): ReplicatedJournal = {

    val log = ActorLog(system, ReplicatedCassandra.getClass)

    val retries = 3

    val statementConfig = StatementConfig(
      idempotent = true, /*TODO remove from here*/
      consistencyLevel = ConsistencyLevel.ONE,
      retryPolicy = new LoggingRetryPolicy(NextHostRetryPolicy(retries)))

    val statements = for {
      tables <- CreateSchema(config.schema, session)
      prepareAndExecute = PrepareAndExecute(session, statementConfig)
      statements <- Statements(tables, prepareAndExecute)
    } yield {
      statements
    }

    apply(statements, config.segmentSize)
  }

  def apply(
    statements: Async[Statements],
    segmentSize: Int): ReplicatedJournal = new ReplicatedJournal {

    def topics() = {
      for {
        statements <- statements
        topics <- statements.selectTopics()
      } yield {
        topics.sorted
      }
    }

    // TODO consider creating collection   Deleted/Nil :: Elem :: Elem
    // TODO to encode sequence that can start either from 0 or for Deleted

    // TODO what if eventualRecords.empty but deletedTo is present ?
    // TODO prevent passing both No records and No deletion
    def save(key: Key, replicate: Replicate, timestamp: Instant): Async[Unit] = {

      def save(statements: Statements, metadata: Option[Metadata]) = {

        def delete(deleteTo: SeqNr, metadata: Metadata, bound: Boolean) = {

          def delete(from: SeqNr) = {

            def delete(deleteTo: SeqNr) = {

              def deleteRecords() = {

                def segment(seqNr: SeqNr) = SegmentNr(seqNr, metadata.segmentSize)

                val asyncs = for {
                  segment <- segment(from) to segment(deleteTo) // TODO maybe add ability to create Seq[Segment] out of SeqRange ?
                } yield {
                  statements.deleteRecords(key, segment, deleteTo)
                }
                Async.foldUnit(asyncs)
              }

              for {
                _ <- statements.updateMetadata(key, Some(deleteTo), timestamp)
                _ <- deleteRecords()
              } yield {}
            }

            if (bound) delete(deleteTo)
            else for {
              last <- LastSeqNr(key, from, metadata, statements.selectLastRecord)
              result <- last match {
                case None       => Async.unit
                case Some(last) => delete(deleteTo min last)
              }
            } yield {
              result
            }
          }

          metadata.deleteTo match {
            case None            => delete(SeqNr.Min)
            case Some(deletedTo) =>
              if (deletedTo >= deleteTo) Async.unit
              else deletedTo.nextOpt match {
                case None       => Async.unit
                case Some(from) => delete(from)
              }
          }
        }


        def save(replicated: List[ReplicatedEvent], deleteTo: Option[SeqNr]) = {

          def saveRecords(segmentSize: Int) = {

            @tailrec
            def loop(
              replicated: List[ReplicatedEvent],
              s: Option[(Segment, Nel[ReplicatedEvent])], // TODO not tuple
              async: Async[Unit]): Async[Unit] = {

              def execute(segment: Segment, events: Nel[ReplicatedEvent]) = {
                val next = statements.insertRecords(key, segment.nr, events)
                for {
                  _ <- async
                  _ <- next
                } yield {}
              }

              replicated match {
                case head :: tail =>
                  val seqNr = head.event.seqNr
                  s match {
                    case Some((segment, events)) => segment.next(seqNr) match {
                      case None =>
                        loop(tail, Some((segment, head :: events)), async)

                      case Some(next) =>
                        loop(tail, Some((next, Nel(head))), execute(segment, events))
                    }
                    case None                    =>
                      loop(tail, Some((Segment(seqNr, segmentSize), Nel(head))), async)
                  }

                case Nil =>
                  s.fold(async) { case (segment, events) => execute(segment, events) }
              }
            }

            loop(replicated, None, Async.unit)
          }

          def updateMetadata(metadata: Metadata) = {
            // TODO do not update deleteTo
            for {
              _ <- statements.updateMetadata(key, metadata.deleteTo, timestamp)
            } yield metadata
          }

          def saveMetadata() = {
            val metadata = Metadata(segmentSize = segmentSize, deleteTo = deleteTo)
            for {
              _ <- statements.insertMetadata(key, metadata, timestamp)
            } yield metadata
          }

          for {
            metadata <- metadata match {
              case None           => saveMetadata()
              case Some(metadata) =>
                for {
                  _ <- deleteTo match {
                    case None           => updateMetadata(metadata)
                    case Some(deleteTo) => delete(deleteTo, metadata, bound = true)
                  }
                } yield metadata
            }
            _ <- saveRecords(metadata.segmentSize)
          } yield {}
        }

        def deleteUnbound(deletedTo: SeqNr) = metadata match {
          case None           => Async.unit
          case Some(metadata) => delete(deletedTo, metadata, bound = false)
        }

        replicate match {
          case Replicate.DeleteToKnown(deletedTo, records) => save(records, deletedTo)
          case Replicate.DeleteUnbound(deletedTo)          => deleteUnbound(deletedTo)
        }
      }

      for {
        statements <- statements
        metadata <- statements.selectMetadata(key)
        result <- save(statements, metadata)
      } yield {
        result
      }
    }

    def save(topic: Topic, topicPointers: TopicPointers): Async[Unit] = {
      val pointers = topicPointers.pointers
      if (pointers.isEmpty) Async.unit
      else {

        // TODO topic is a partition key, should I batch by partition ?
        val timestamp = Instant.now() // TODO pass as argument

        def savePointers(statements: Statements) = {
          val asyncs = for {
            (partition, offset) <- pointers
          } yield {
            val insert = PointerInsert(
              topic = topic,
              partition = partition,
              offset = offset,
              updated = timestamp,
              created = timestamp)

            statements.insertPointer(insert)
          }

          Async.foldUnit(asyncs)
        }

        for {
          statements <- statements
          _ <- savePointers(statements)
        } yield ()
      }
    }

    def pointers(topic: Topic) = {
      for {
        statements <- statements
        topicPointers <- statements.selectPointers(topic)
      } yield {
        topicPointers
      }
    }
  }


  final case class Statements(
    insertRecords: JournalStatement.InsertRecords.Type,
    selectLastRecord: JournalStatement.SelectLastRecord.Type,
    deleteRecords: JournalStatement.DeleteRecords.Type,
    insertMetadata: MetadataStatement.Insert.Type,
    selectMetadata: MetadataStatement.Select.Type,
    updateMetadata: MetadataStatement.Update.Type,
    insertPointer: PointerStatement.Insert.Type,
    selectPointers: PointerStatement.SelectPointers.Type,
    selectTopics: PointerStatement.SelectTopics.Type)

  object Statements {

    def apply(tables: Tables, prepareAndExecute: PrepareAndExecute): Async[Statements] = {

      val insertRecords = JournalStatement.InsertRecords(tables.journal, prepareAndExecute)
      val selectLastRecord = JournalStatement.SelectLastRecord(tables.journal, prepareAndExecute)
      val deleteRecords = JournalStatement.DeleteRecords(tables.journal, prepareAndExecute)
      val insertMetadata = MetadataStatement.Insert(tables.metadata, prepareAndExecute)
      val selectMetadata = MetadataStatement.Select(tables.metadata, prepareAndExecute)
      val updateMetadata = MetadataStatement.Update(tables.metadata, prepareAndExecute)
      val insertPointer = PointerStatement.Insert(tables.pointer, prepareAndExecute)
      val selectPointers = PointerStatement.SelectPointers(tables.pointer, prepareAndExecute)
      val selectTopics = PointerStatement.SelectTopics(tables.pointer, prepareAndExecute)

      for {
        insertRecords <- insertRecords
        selectLastRecord <- selectLastRecord
        deleteRecords <- deleteRecords
        insertMetadata <- insertMetadata
        selectMetadata <- selectMetadata
        updateMetadata <- updateMetadata
        insertPointer <- insertPointer
        selectPointers <- selectPointers
        selectTopics <- selectTopics
      } yield {
        Statements(
          insertRecords = insertRecords,
          selectLastRecord = selectLastRecord,
          deleteRecords = deleteRecords,
          insertMetadata = insertMetadata,
          selectMetadata = selectMetadata,
          updateMetadata = updateMetadata,
          insertPointer = insertPointer,
          selectPointers = selectPointers,
          selectTopics = selectTopics)
      }
    }
  }
}