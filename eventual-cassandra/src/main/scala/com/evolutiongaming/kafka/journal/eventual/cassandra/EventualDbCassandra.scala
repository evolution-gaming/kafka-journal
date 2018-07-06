package com.evolutiongaming.kafka.journal.eventual.cassandra

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import com.datastax.driver.core.policies.{LoggingRetryPolicy, RetryPolicy}
import com.datastax.driver.core.{Metadata => _, _}
import com.evolutiongaming.cassandra.Helpers._
import com.evolutiongaming.cassandra.NextHostRetryPolicy
import com.evolutiongaming.kafka.journal.Alias._
import com.evolutiongaming.kafka.journal.SeqRange
import com.evolutiongaming.kafka.journal.eventual._
import com.evolutiongaming.skafka.Topic

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

    implicit val materializer = ActorMaterializer()

    val retries = 3

    val statementConfig = StatementConfig(
      idempotent = true, /*TODO remove from here*/
      consistencyLevel = ConsistencyLevel.ONE,
      retryPolicy = new LoggingRetryPolicy(NextHostRetryPolicy(retries)))

    val keyspace = schemaConfig.keyspace

    val journalName = TableName(keyspace = keyspace.name, table = schemaConfig.journalName)

    val metadataName = TableName(keyspace = keyspace.name, table = schemaConfig.metadataName)

    val pointerName = TableName(keyspace = keyspace.name, table = schemaConfig.pointerName)

    val futureUnit = Future.successful(())

    // TODO moveout
    case class PreparedStatements(
      insertRecord: JournalStatement.InsertRecord.Type,
      selectLastRecord: JournalStatement.SelectLastRecord.Type,
      selectRecords: JournalStatement.SelectRecords.Type,
      insertMetadata: MetadataStatement.Insert.Type,
      selectMetadata: MetadataStatement.Select.Type,
      selectSegmentSize: MetadataStatement.SelectSegmentSize.Type,
      updatedDeletedTo: MetadataStatement.UpdatedMetadata.Type,
      insertPointer: PointerStatement.Insert.Type,
      updatePointer: PointerStatement.Update.Type,
      selectPointer: PointerStatement.Select.Type,
      selectTopicPointer: PointerStatement.SelectTopicPointers.Type)

    def createKeyspace() = {
      // TODO make sure two parallel instances does not do the same
      val query = JournalStatement.createKeyspace(keyspace)
      session.executeAsync(query).asScala()
    }

    def createTable() = {

      val journal = {
        val query = JournalStatement.createTable(journalName)
        session.executeAsync(query).asScala()
      }

      val metadata = {
        val query = MetadataStatement.createTable(metadataName)
        session.executeAsync(query).asScala()
      }

      val pointer = {
        val query = PointerStatement.createTable(pointerName)
        session.executeAsync(query).asScala()
      }

      for {
        _ <- journal
        _ <- metadata
        _ <- pointer
      } yield {

      }
    }


    def preparedStatements() = {

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

      val insertRecord = JournalStatement.InsertRecord(journalName, session.prepareAsync(_: String).asScala())
      val selectLastRecord = JournalStatement.SelectLastRecord(journalName, prepareAndExecute)
      val listRecords = JournalStatement.SelectRecords(journalName, prepareAndExecute)
      val insertMetadata = MetadataStatement.Insert(metadataName, prepareAndExecute)
      val selectMetadata = MetadataStatement.Select(metadataName, prepareAndExecute)
      val selectSegmentSize = MetadataStatement.SelectSegmentSize(metadataName, prepareAndExecute)
      val updatedDeletedTo = MetadataStatement.UpdatedMetadata(metadataName, prepareAndExecute)
      val insertPointer = PointerStatement.Insert(pointerName, prepareAndExecute)
      val updatePointer = PointerStatement.Update(pointerName, prepareAndExecute)
      val selectPointer = PointerStatement.Select(pointerName, prepareAndExecute)
      val selectTopicPointers = PointerStatement.SelectTopicPointers(pointerName, prepareAndExecute)

      for {
        insertRecord <- insertRecord
        selectLastRecord <- selectLastRecord
        listRecords <- listRecords
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
          listRecords,
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
      _ <- if (keyspace.autoCreate) createKeyspace() else futureUnit
      _ <- if (schemaConfig.autoCreate) createTable() else futureUnit
      preparedStatements <- preparedStatements()
    } yield {
      (session, preparedStatements)
    }

    // TODO remove
    def segmentSize(id: Id, prepared: PreparedStatements): Future[Int] = {
      val selectSegmentSize = prepared.selectSegmentSize
      for {
        segmentSize <- selectSegmentSize(id)
      } yield {
        segmentSize getOrElse config.segmentSize
      }
    }

    def metadata(id: Id, prepared: PreparedStatements) = {
      val selectMetadata = prepared.selectMetadata
      for {
        metadata <- selectMetadata(id)
      } yield {
        // TODO what to do if it is empty?

        if (metadata.isEmpty) println(s"$id metadata is empty")

        metadata
      }
    }


    new EventualDb {

      // TODO consider creating collection   Deleted/Nil :: Elem :: Elem
      // TODO to encode sequence that can start either from 0 or for Deleted

      // TODO verify all records have same id
      // TODO what if eventualRecords.empty but deletedTo is present ?
      // TODO prevent passing both No records and No deletion
      def save(id: Id, eventualRecords: UpdateTmp, topic: Topic): Future[Unit] = {


        def delete(seqNr: SeqNr) = {
          // TODO implement
          Future.unit
        }







        def save1(eventualRecords: EventualRecords) = {

          println(s"$id save deletedTo: ${ eventualRecords.deletedTo }, topic: $topic")

          // TODO delete actual records !!!

          val records = eventualRecords.records

          def insert(prepared: PreparedStatements, segmentSize: Int) = {
            val records = eventualRecords.records
            if (records.isEmpty) Future.unit
            else {

              val head = records.head
              val segment = Segment(head.seqNr, config.segmentSize)
              val statement = {

                val statements = for {
                  record <- records
                } yield {
                  prepared.insertRecord(record, segment)
                }

                if (statements.size == 1) {
                  statements.head
                } else {
                  val batch = new BatchStatement()
                  statements.foldLeft(batch) { _ add _ }
                }
              }

              val statementFinal = statement.set(statementConfig)
              session.executeAsync(statementFinal).asScala()
            }
          }

          def insertRecordsAndMetadata(prepared: PreparedStatements, metadata: Option[Metadata]) = {
            val head = records.head.seqNr
            val last = records.last.seqNr
            val range = SeqRange(from = head, to = last)

            // TODO
            val segmentSize = metadata.map{_.segmentSize} getOrElse config.segmentSize

            val metadata2 = Metadata(
              id = id,
              topic = topic,
              range = range,
              segmentSize = segmentSize)

            for {
              //                    _ <- deleteResult
              _ <- prepared.insertMetadata(metadata2)
              _ <- insert(prepared, segmentSize)
            } yield {

            }
          }

          eventualRecords.deletedTo match {
            case None =>

              for {
                (session, prepared) <- sessionAndPreparedStatements
                metadata <- prepared.selectMetadata(id)
                _ <- insertRecordsAndMetadata(prepared, metadata)
              } yield {

              }


            case Some(deletedTo) =>
              
              def save(metadata: Option[Metadata], prepared: PreparedStatements) = {

                val deleteResult = metadata match {
                  case Some(metadata) => delete(deletedTo)
                  case None => Future.unit
                }

                def tmp() = {

                  // TODO this is delete all use case we don't need to insert segment size,
                  if (records.isEmpty) {

                    val range = SeqRange(from = deletedTo, to = deletedTo)
                    // TODO
                    val segmentSize = metadata.map { _.segmentSize } getOrElse config.segmentSize

                    val metadata2 = Metadata(
                      id = id,
                      topic = topic,
                      range = range,
                      segmentSize = segmentSize)

                    for {
                      //                    _ <- deleteResult
                      _ <- prepared.insertMetadata(metadata2)
                    } yield {

                    }

                  } else {
                    insertRecordsAndMetadata(prepared, metadata)
                  }
                }

                for {
                  _ <- deleteResult
                  _ <- tmp()
                } yield {

                }
              }


              for {
                (session, prepared) <- sessionAndPreparedStatements
                metadata <- prepared.selectMetadata(id)
                _ <- save(metadata, prepared)
              } yield {

              }

          }
        }

        def save2(deletedTo: SeqNr) = {

          def save2(prepared: PreparedStatements, metadata: Option[Metadata]) = {
            metadata match {
              case None => Future.unit
              case Some(metadata) =>

                val range = metadata.range

                if(deletedTo < range.from) {
                  Future.unit
                } else {
                  val range2 = {
                    if (deletedTo > range.to) {
                      val start = range.to + 1
                      SeqRange(from = start, to = start)
                    } else {
                      range.copy(from = deletedTo + 1)
                    }
                  }

//                  val deletedTo2 = deletedTo + 1
//                  val start = deletedTo2 min range.to
//                  val range2 = range.copy(from = start)
                  println(s"$id ############### $range2")

                  val metadata2 = Metadata(
                    id = id,
                    topic = topic,
                    range = range2,
                    segmentSize = metadata.segmentSize)

                  prepared.insertMetadata(metadata2)
                }
            }
          }

          if(deletedTo == SeqNr.Min) {
            Future.unit
          } else {
            for {
              (session, prepared) <- sessionAndPreparedStatements
              metadata <- prepared.selectMetadata(id)
              _ <- save2(prepared, metadata)
            } yield {

            }
          }
        }

        eventualRecords match {
          case UpdateTmp.DeleteToKnown(deletedTo, records) => save1(EventualRecords(records, Some(deletedTo)))
          case UpdateTmp.DeleteToUnknown(deletedTo) => save2(deletedTo)
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


      // TODO return closest offset
      def pointerOld(id: Id, from: SeqNr): Future[Option[Pointer]] = {
        println(s"$id EventualCassandra.last from: $from")

        def pointer(statement: JournalStatement.SelectLastRecord.Type, segmentSize: Int, metadata: Option[Metadata]) = {

//          val deletedTo = metadata.map { _.deleteTo } getOrElse 0L

          //          val seqNr = from max deletedTo
          //
          //          val partition: Partition = ???


          def recur(from: SeqNr, prev: Option[(Segment, Pointer)]): Future[Option[Pointer]] = {
            // println(s"EventualCassandra.last.recur id: $id, segment: $segment")

            def record = prev.map { case (_, record) => record }

            // TODO use deletedTo
            val segment = Segment(from, segmentSize)
            if (prev.exists { case (segmentPrev, _) => segmentPrev == segment }) {
              Future.successful(record)
            } else {
              for {
                result <- statement(id, segment, from)
                result <- result match {
                  case None         => Future.successful(record)
                  case Some(result) =>
                    val segmentAndRecord = (segment, result)
                    recur(from.next, Some(segmentAndRecord))
                }
              } yield {
                result
              }
            }
          }

          recur(from, None)
        }

        for {
          (session, statements) <- sessionAndPreparedStatements
          segmentSize <- segmentSize(id, statements)
          metadata <- metadata(id, statements)
          result <- pointer(statements.selectLastRecord, segmentSize, metadata)
        } yield {
          result
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