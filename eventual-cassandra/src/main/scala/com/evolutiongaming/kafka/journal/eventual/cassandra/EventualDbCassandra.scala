package com.evolutiongaming.kafka.journal.eventual.cassandra

import java.time.Instant

import akka.actor.ActorSystem
import com.datastax.driver.core.policies.LoggingRetryPolicy
import com.datastax.driver.core.{Metadata => _, _}
import com.evolutiongaming.cassandra.CassandraHelper._
import com.evolutiongaming.cassandra.NextHostRetryPolicy
import com.evolutiongaming.kafka.journal.Alias._
import com.evolutiongaming.kafka.journal.FutureHelper._
import com.evolutiongaming.kafka.journal.eventual._
import com.evolutiongaming.kafka.journal.eventual.cassandra.CassandraHelper._
import com.evolutiongaming.safeakka.actor.ActorLog
import com.evolutiongaming.skafka.Topic

import scala.collection.immutable.Seq
import scala.concurrent.{ExecutionContext, Future}


// TODO create collection that is optimised for ordered sequence and seqNr
// TODO redesign EventualDbCassandra so it can hold stat and called recursively
// TODO rename
object EventualDbCassandra {

  def apply(
    session: Session,
    schemaConfig: SchemaConfig,
    config: EventualCassandraConfig)(implicit system: ActorSystem, ec: ExecutionContext): EventualDb = {

    val log = ActorLog(system, EventualDbCassandra.getClass)

    val retries = 3

    val statementConfig = StatementConfig(
      idempotent = true, /*TODO remove from here*/
      consistencyLevel = ConsistencyLevel.ONE,
      retryPolicy = new LoggingRetryPolicy(NextHostRetryPolicy(retries)))

    val sessionAndStatements = for {
      tables <- CreateSchema(schemaConfig, session)
      prepareAndExecute = PrepareAndExecute(session, statementConfig)
      statements <- Statements(session, tables, prepareAndExecute)
    } yield {
      (session, statements)
    }


    new EventualDb {

      // TODO consider creating collection   Deleted/Nil :: Elem :: Elem
      // TODO to encode sequence that can start either from 0 or for Deleted

      // TODO verify all records have same id
      // TODO what if eventualRecords.empty but deletedTo is present ?
      // TODO prevent passing both No records and No deletion
      def save(id: Id, updateTmp: UpdateTmp, topic: Topic): Future[Unit] = {

        def save(statements: Statements, metadata: Option[Metadata], session: Session) = {

          def delete(deletedTo: SeqNr, metadata: Metadata) = {
            if (metadata.deletedTo >= deletedTo) Future.unit
            else {

              def segmentOf(seqNr: SeqNr) = Segment(seqNr, metadata.segmentSize)

              def delete(segment: Segment) = {
                statements.deleteRecords(id, segment, deletedTo)
              }

              val lowest = segmentOf(metadata.deletedTo)
              val highest = segmentOf(deletedTo)
              val futures = for {
                segment <- lowest to highest // TODO maybe add ability to create Seq[Segment] out of SeqRange ?
              } yield {
                delete(segment)
              }
              Future.sequence(futures).unit
            }
          }


          def save(records: Seq[EventualRecord], deletedTo: Option[SeqNr]) = {

            def saveRecords(segmentSize: Int) = {
              if (records.isEmpty) Future.unit
              else {
                val head = records.head
                val segment = Segment(head.seqNr, config.segmentSize)
                // TODO rename
                val insert = {

                  val inserts = for {
                    record <- records
                  } yield {
                    statements.insertRecord(record, segment)
                  }

                  if (inserts.size == 1) {
                    inserts.head
                  } else {
                    val batch = new BatchStatement()
                    inserts.foldLeft(batch) { _ add _ }
                  }
                }

                val insertFinal = insert.set(statementConfig)
                session.executeAsync(insertFinal).asScala()
              }
            }

            def saveMetadata(): Future[Metadata] = {

              val segmentSize = metadata.fold(config.segmentSize)(_.segmentSize)

              val deletedTo2 = deletedTo getOrElse SeqNr.Min
              val deletedTo3 = metadata.fold(deletedTo2)(_.deletedTo max deletedTo2)

              val metadataNew = Metadata(
                id = id,
                topic = topic,
                segmentSize = segmentSize,
                deletedTo = deletedTo3)

              // TODO split on insert and update queries
              for {
                _ <- statements.insertMetadata(metadataNew)
              } yield {
                metadataNew
              }
            }

            for {
              metadata <- saveMetadata()
              _ <- deletedTo.fold(Future.unit) { deletedTo => delete(deletedTo, metadata) }
              _ <- saveRecords(metadata.segmentSize)
            } yield {}
          }

          def deleteUnbound(deleteTo: SeqNr) = {

            metadata.fold(Future.unit) { metadata =>
              if (deleteTo <= metadata.deletedTo) {
                Future.unit
              } else {

                def saveAndDelete(deletedTo: SeqNr) = {
                  val metadataNew = Metadata(
                    id = id,
                    topic = topic,
                    deletedTo = deletedTo,
                    segmentSize = metadata.segmentSize)

                  for {
                    _ <- statements.insertMetadata(metadataNew) // TODO optimise query
                    _ <- delete(deletedTo, metadata)
                  } yield {}
                }

                for {
                  lastSeqNr <- LastSeqNr(id, metadata.deletedTo, statements.selectLastRecord, metadata)
                  result <- saveAndDelete(deleteTo min lastSeqNr)
                } yield {
                  result
                }
              }
            }
          }

          updateTmp match {
            case UpdateTmp.DeleteToKnown(deletedTo, records) => save(records, deletedTo)
            case UpdateTmp.DeleteUnbound(deletedTo)          => deleteUnbound(deletedTo)
          }
        }

        for {
          (session, statements) <- sessionAndStatements
          metadata <- statements.selectMetadata(id)
          result <- save(statements, metadata, session)
        } yield {
          result
        }
      }

      def savePointers(topic: Topic, topicPointers: TopicPointers): Future[Unit] = {
        val pointers = topicPointers.pointers
        if (pointers.isEmpty) Future.unit
        else {

          // TODO topic is a partition key, should I batch by partition ?
          val timestamp = Instant.now() // TODO pass as argument

          def savePointers(statements: Statements) = {
            val futures = for {
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

            Future.sequence(futures)
          }

          for {
            (_, statements) <- sessionAndStatements
            result <- savePointers(statements)
          } yield {
            result
          }
        }
      }


      def pointers(topic: Topic): Future[TopicPointers] = {
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
    selectSegmentSize: MetadataStatement.SelectSegmentSize.Type,
    updatedDeletedTo: MetadataStatement.UpdatedMetadata.Type,
    insertPointer: PointerStatement.Insert.Type,
    updatePointer: PointerStatement.Update.Type,
    selectPointer: PointerStatement.Select.Type,
    selectTopicPointer: PointerStatement.SelectTopicPointers.Type)

  object Statements {

    def apply(
      session: Session,
      tables: Tables,
      prepareAndExecute: PrepareAndExecute)(implicit
      ec: ExecutionContext): Future[Statements] = {

      val insertRecord = JournalStatement.InsertRecord(tables.journal, session.prepareAsync(_: String).asScala())
      val selectLastRecord = JournalStatement.SelectLastRecord(tables.journal, prepareAndExecute)
      val selectRecords = JournalStatement.SelectRecords(tables.journal, prepareAndExecute)
      val deleteRecords = JournalStatement.DeleteRecords(tables.journal, prepareAndExecute)
      val insertMetadata = MetadataStatement.Insert(tables.metadata, prepareAndExecute)
      val selectMetadata = MetadataStatement.Select(tables.metadata, prepareAndExecute)
      val selectSegmentSize = MetadataStatement.SelectSegmentSize(tables.metadata, prepareAndExecute)
      val updatedDeletedTo = MetadataStatement.UpdatedMetadata(tables.metadata, prepareAndExecute)
      val insertPointer = PointerStatement.Insert(tables.pointer, prepareAndExecute)
      val updatePointer = PointerStatement.Update(tables.pointer, prepareAndExecute)
      val selectPointer = PointerStatement.Select(tables.pointer, prepareAndExecute)
      val selectTopicPointers = PointerStatement.SelectTopicPointers(tables.pointer, prepareAndExecute)

      for {
        insertRecord <- insertRecord
        selectLastRecord <- selectLastRecord
        selectRecords <- selectRecords
        deleteRecords <- deleteRecords
        insertMetadata <- insertMetadata
        selectMetadata <- selectMetadata
        selectSegmentSize <- selectSegmentSize
        updatedDeletedTo <- updatedDeletedTo
        insertPointer <- insertPointer
        updatePointer <- updatePointer
        selectPointer <- selectPointer
        selectTopicPointers <- selectTopicPointers
      } yield {
        Statements(
          insertRecord,
          selectLastRecord,
          selectRecords,
          deleteRecords,
          insertMetadata,
          selectMetadata,
          selectSegmentSize,
          updatedDeletedTo,
          insertPointer,
          updatePointer,
          selectPointer,
          selectTopicPointers)
      }
    }
  }
}