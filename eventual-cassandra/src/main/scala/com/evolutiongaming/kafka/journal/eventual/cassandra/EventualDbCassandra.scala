package com.evolutiongaming.kafka.journal.eventual.cassandra

import akka.actor.ActorSystem
import com.datastax.driver.core.policies.{LoggingRetryPolicy, RetryPolicy}
import com.datastax.driver.core.{Metadata => _, _}
import com.evolutiongaming.cassandra.CassandraHelpers._
import com.evolutiongaming.cassandra.NextHostRetryPolicy
import com.evolutiongaming.kafka.journal.Alias._
import com.evolutiongaming.kafka.journal.FutureHelper._
import com.evolutiongaming.kafka.journal.eventual._
import com.evolutiongaming.safeakka.actor.ActorLog
import com.evolutiongaming.skafka.Topic

import scala.collection.immutable.Seq
import scala.concurrent.{ExecutionContext, Future}


// TODO create collection that is optimised for ordered sequence and seqNr
// TODO redesign EventualDbCassandra so it can hold stat and called recursively
object EventualDbCassandra {

  case class StatementConfig(
    idempotent: Boolean = false,
    consistencyLevel: ConsistencyLevel,
    retryPolicy: RetryPolicy)


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

    // TODO moveout
    case class PreparedStatements(
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


    def preparedStatements(tables: Tables) = {

      val prepareAndExecute = new PrepareAndExecute {

        def prepare(query: String) = {
          session.prepareAsync(query).asScala()
        }

        def execute(statement: BoundStatement) = {
          val statementConfigured = statement.set(statementConfig)
          val result = session.executeAsync(statementConfigured)
          result.asScala()
        }
      }

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
        PreparedStatements(
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

    val sessionAndPreparedStatements = for {
      tables <- CreateSchema(schemaConfig, session)
      preparedStatements <- preparedStatements(tables)
    } yield {
      (session, preparedStatements)
    }


    new EventualDb {

      // TODO consider creating collection   Deleted/Nil :: Elem :: Elem
      // TODO to encode sequence that can start either from 0 or for Deleted

      // TODO verify all records have same id
      // TODO what if eventualRecords.empty but deletedTo is present ?
      // TODO prevent passing both No records and No deletion
      def save(id: Id, updateTmp: UpdateTmp, topic: Topic): Future[Unit] = {

        def save(statements: PreparedStatements, metadata: Option[Metadata]) = {

          def delete(deletedTo: SeqNr, metadata: Metadata) = {

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


          def save(records: Seq[EventualRecord], deletedTo: Option[SeqNr]) = {

            println(s"$id save deletedTo: $deletedTo, topic: $topic")

            // TODO delete actual records !!!

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

              val segmentSize = metadata.fold(config.segmentSize) { _.segmentSize }

              val metadataNew = Metadata(
                id = id,
                topic = topic,
                segmentSize = segmentSize,
                deletedTo = deletedTo getOrElse SeqNr.Min)

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

          def deleteUnbound(deletedToUnbound: SeqNr) = {

            metadata.fold(Future.unit) { metadata =>
              if (deletedToUnbound <= metadata.deletedTo) {
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
                  deletedTo = deletedToUnbound min lastSeqNr
                  result <- saveAndDelete(deletedTo)
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
          (session, statements) <- sessionAndPreparedStatements
          metadata <- statements.selectMetadata(id)
          result <- save(statements, metadata)
        } yield {
          result
        }
      }

      def savePointers(updatePointers: UpdatePointers): Future[Unit] = {
        val pointers = updatePointers.pointers
        if (pointers.isEmpty) Future.unit
        else {

          // TODO topic is a partition key, should I batch by partition ?

          def savePointers(prepared: PreparedStatements) = {
            val updated = updatePointers.timestamp
            val futures = for {
              (topicPartition, (offset, created)) <- pointers
            } yield {
              val topic = topicPartition.topic
              val partition = topicPartition.partition

              // TODO no need to pass separate created timestamp
              created match {
                case None =>
                  val update = PointerUpdate(
                    topic = topic,
                    partition = partition,
                    offset = offset,
                    updated = updated)

                  prepared.updatePointer(update)

                case Some(created) =>
                  val insert = PointerInsert(
                    topic = topic,
                    partition = partition,
                    offset = offset,
                    updated = updated,
                    created = created)

                  prepared.insertPointer(insert)
              }
            }

            Future.sequence(futures)
          }

          for {
            (session, prepared) <- sessionAndPreparedStatements
            _ <- savePointers(prepared)
          } yield {

          }
        }
      }


      def topicPointers(topic: Topic): Future[TopicPointers] = {
        for {
          (session, prepared) <- sessionAndPreparedStatements
          topicPointers <- prepared.selectTopicPointer(topic)
        } yield {
          topicPointers
        }
      }
    }
  }


  implicit class StatementOps(val self: Statement) extends AnyVal {

    def set(statementConfig: StatementConfig): Statement = {
      self
        .setIdempotent(statementConfig.idempotent)
        .setConsistencyLevel(statementConfig.consistencyLevel)
        .setRetryPolicy(statementConfig.retryPolicy)
    }
  }

}