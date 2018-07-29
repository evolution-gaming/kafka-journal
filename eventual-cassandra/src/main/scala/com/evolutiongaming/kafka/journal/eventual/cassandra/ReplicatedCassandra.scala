package com.evolutiongaming.kafka.journal.eventual.cassandra

import java.time.Instant

import akka.actor.ActorSystem
import com.datastax.driver.core.policies.LoggingRetryPolicy
import com.datastax.driver.core.{BatchStatement, BoundStatement, ConsistencyLevel}
import com.evolutiongaming.cassandra.{NextHostRetryPolicy, Session}
import com.evolutiongaming.concurrent.async.Async
import com.evolutiongaming.concurrent.async.AsyncConverters._
import com.evolutiongaming.kafka.journal.Alias._
import com.evolutiongaming.kafka.journal.eventual._
import com.evolutiongaming.kafka.journal.eventual.cassandra.CassandraHelper._
import com.evolutiongaming.kafka.journal.{Key, ReplicatedEvent}
import com.evolutiongaming.nel.Nel
import com.evolutiongaming.safeakka.actor.ActorLog
import com.evolutiongaming.skafka.Topic

import scala.annotation.tailrec
import scala.concurrent.ExecutionContext


// TODO create collection that is optimised for ordered sequence and seqNr
// TODO redesign EventualDbCassandra so it can hold stat and called recursively
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

    val sessionAndStatements = for {
      tables <- CreateSchema(config.schema, session)
      prepareAndExecute = PrepareAndExecute(session, statementConfig)
      statements <- Statements(session, tables, prepareAndExecute)
    } yield {
      (session, statements)
    }


    new ReplicatedJournal {

      def topics() = {
        for {
          (_, statements) <- sessionAndStatements
          topics <- statements.selectTopics()
        } yield {
          topics
        }
      }

      // TODO consider creating collection   Deleted/Nil :: Elem :: Elem
      // TODO to encode sequence that can start either from 0 or for Deleted

      // TODO what if eventualRecords.empty but deletedTo is present ?
      // TODO prevent passing both No records and No deletion
      def save(key: Key, replicate: Replicate, timestamp: Instant): Async[Unit] = {

        def save(statements: Statements, metadata: Option[Metadata], session: Session) = {

          def delete(deleteTo: SeqNr, metadata: Metadata) = {
            if (metadata.deleteTo >= deleteTo) Async.unit
            else {

              // TODO
              def segmentOf(seqNr: SeqNr) = SegmentNr(seqNr, metadata.segmentSize)

              def delete(segment: SegmentNr) = {
                statements.deleteRecords(key, segment, deleteTo)
              }

              val lowest = segmentOf(metadata.deleteTo)
              val highest = segmentOf(deleteTo)
              val asyncs = for {
                segment <- lowest to highest // TODO maybe add ability to create Seq[Segment] out of SeqRange ?
              } yield {
                delete(segment)
              }
              Async.foldUnit(asyncs).unit
            }
          }


          def save(replicated: List[ReplicatedEvent], deleteTo: Option[SeqNr]) = {

            def saveRecords(segmentSize: Int) = {

              @tailrec
              def loop(replicated: List[ReplicatedEvent], s: Option[(SegmentNr, Nel[BoundStatement])], async: Async[Unit]): Async[Unit] = {

                def execute(statements: Nel[BoundStatement]) = {

                  def execute() = {
                    val statement = {
                      if (statements.tail.isEmpty) statements.head
                      else statements.foldLeft(new BatchStatement()) { _ add _ }
                    }

                    val statementFinal = statement.set(statementConfig)
                    session.execute(statementFinal).async.unit
                  }

                  for {
                    _ <- async
                    _ <- execute()
                  } yield {}
                }

                if (replicated.isEmpty) {
                  s.fold(async) { case (_, statements) => execute(statements) }
                } else {
                  val head = replicated.head
                  val segment = SegmentNr(head.event.seqNr, config.segmentSize)
                  val statement = statements.insertRecord(key, head, segment)

                  s match {
                    case Some((segmentPrev, statements)) =>
                      if (segmentPrev == segment) {
                        val s = (segment, statement :: statements)
                        loop(replicated.tail, Some(s), async)
                      } else {
                        val s = (segment, Nel(statement))
                        loop(replicated.tail, Some(s), execute(statements))
                      }

                    case None =>
                      val s = (segment, Nel(statement))
                      loop(replicated.tail, Some(s), async)
                  }
                }
              }

              loop(replicated, None, Async.unit)
            }

            def saveMetadata(): Async[Metadata] = {

              def insert() = {
                val metadata = Metadata(
                  segmentSize = config.segmentSize,
                  deleteTo = deleteTo getOrElse SeqNr.Min)

                for {
                  _ <- statements.insertMetadata(key, metadata, timestamp)
                } yield metadata
              }

              def update(metadata: Metadata) = {
                val deleteTo2 = deleteTo getOrElse SeqNr.Min
                val deleteTo3 = metadata.deleteTo max deleteTo2
                for {
                  _ <- statements.updateMetadata(key, deleteTo3, timestamp)
                } yield metadata.copy(deleteTo = deleteTo3)
              }

              metadata.fold(insert())(update)
            }

            for {
              metadata <- saveMetadata()
              _ <- deleteTo.fold(Async.unit) { deletedTo => delete(deletedTo, metadata) }
              _ <- saveRecords(metadata.segmentSize)
            } yield {}
          }

          def deleteUnbound(deleteTo: SeqNr) = {

            metadata.fold(Async.unit) { metadata =>
              if (deleteTo <= metadata.deleteTo) Async.unit
              else {

                def saveAndDelete(deleteTo: SeqNr) = {
                  for {
                    _ <- statements.updateMetadata(key, deleteTo, timestamp)
                    _ <- delete(deleteTo, metadata)
                  } yield {}
                }

                for {
                  lastSeqNr <- LastSeqNr(key, metadata.deleteTo, statements.selectLastRecord, metadata)
                  result <- saveAndDelete(deleteTo min lastSeqNr)
                } yield {
                  result
                }
              }
            }
          }

          replicate match {
            case Replicate.DeleteToKnown(deletedTo, records) => save(records, deletedTo)
            case Replicate.DeleteUnbound(deletedTo)          => deleteUnbound(deletedTo)
          }
        }

        for {
          (session, statements) <- sessionAndStatements
          metadata <- statements.selectMetadata(key)
          result <- save(statements, metadata, session)
        } yield {
          result
        }
      }

      def savePointers(topic: Topic, topicPointers: TopicPointers): Async[Unit] = {
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
            (_, statements) <- sessionAndStatements
            _ <- savePointers(statements)
          } yield ()
        }
      }


      def pointers(topic: Topic) = {
        for {
          (_, statements) <- sessionAndStatements
          topicPointers <- statements.selectTopicPointer(topic)
        } yield {
          topicPointers
        }
      }
    }
  }


  final case class Statements(
    insertRecord: JournalStatement.InsertRecord.Type,
    selectLastRecord: JournalStatement.SelectLastRecord.Type,
    selectRecords: JournalStatement.SelectRecords.Type,
    deleteRecords: JournalStatement.DeleteRecords.Type,
    insertMetadata: MetadataStatement.Insert.Type,
    selectMetadata: MetadataStatement.Select.Type,
    updateMetadata: MetadataStatement.Update.Type,
    insertPointer: PointerStatement.Insert.Type,
    updatePointer: PointerStatement.Update.Type,
    selectPointer: PointerStatement.Select.Type,
    selectTopicPointer: PointerStatement.SelectTopicPointers.Type,
    selectTopics: PointerStatement.SelectTopics.Type)

  object Statements {

    def apply(
      session: Session,
      tables: Tables,
      prepareAndExecute: PrepareAndExecute)(implicit
      ec: ExecutionContext): Async[Statements] = {

      val insertRecord = JournalStatement.InsertRecord(tables.journal, session.prepare(_: String).async)
      val selectLastRecord = JournalStatement.SelectLastRecord(tables.journal, prepareAndExecute)
      val selectRecords = JournalStatement.SelectRecords(tables.journal, prepareAndExecute)
      val deleteRecords = JournalStatement.DeleteRecords(tables.journal, prepareAndExecute)
      val insertMetadata = MetadataStatement.Insert(tables.metadata, prepareAndExecute)
      val selectMetadata = MetadataStatement.Select(tables.metadata, prepareAndExecute)
      val updateMetadata = MetadataStatement.Update(tables.metadata, prepareAndExecute)
      val insertPointer = PointerStatement.Insert(tables.pointer, prepareAndExecute)
      val updatePointer = PointerStatement.Update(tables.pointer, prepareAndExecute)
      val selectPointer = PointerStatement.Select(tables.pointer, prepareAndExecute)
      val selectTopicPointers = PointerStatement.SelectTopicPointers(tables.pointer, prepareAndExecute)
      val selectTopics = PointerStatement.SelectTopics(tables.pointer, prepareAndExecute)

      for {
        insertRecord <- insertRecord
        selectLastRecord <- selectLastRecord
        selectRecords <- selectRecords
        deleteRecords <- deleteRecords
        insertMetadata <- insertMetadata
        selectMetadata <- selectMetadata
        updateMetadata <- updateMetadata
        insertPointer <- insertPointer
        updatePointer <- updatePointer
        selectPointer <- selectPointer
        selectTopicPointers <- selectTopicPointers
        selectTopics <- selectTopics
      } yield {
        Statements(
          insertRecord,
          selectLastRecord,
          selectRecords,
          deleteRecords,
          insertMetadata,
          selectMetadata,
          updateMetadata,
          insertPointer,
          updatePointer,
          selectPointer,
          selectTopicPointers,
          selectTopics)
      }
    }
  }
}