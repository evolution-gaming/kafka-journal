package com.evolutiongaming.kafka.journal.eventual.cassandra

import java.time.Instant

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Sink, Source}
import com.datastax.driver.core.policies.{LoggingRetryPolicy, RetryPolicy}
import com.datastax.driver.core.{Metadata => _, _}
import com.evolutiongaming.cassandra.Helpers._
import com.evolutiongaming.cassandra.NextHostRetryPolicy
import com.evolutiongaming.kafka.journal.Alias._
import com.evolutiongaming.kafka.journal.SeqRange
import com.evolutiongaming.kafka.journal.eventual._
import com.evolutiongaming.skafka.{Offset, Partition, Topic}

import scala.collection.immutable.Seq
import scala.concurrent.{ExecutionContext, Future}


// TODO create collection that is optimised for ordered sequence and seqNr
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

      // TODO verify all records have same id
      def save(records: Seq[EventualRecord], topic: Topic): Future[Unit] = {

        //        Thread.sleep(400)

        if (records.isEmpty) futureUnit
        else {

          val head = records.head
          val segment = Segment(head.seqNr, config.segmentSize)
          val id = head.id

          def statement(prepared: PreparedStatements, segmentSize: Int) = {

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

          def insertMetadata(prepared: PreparedStatements) = {
            // TODO make constant of SeqNr 1
            if (head.seqNr == 1) {
              val timestamp = Instant.now()
              val segmentSize = config.segmentSize
              // TODO is it right to use `currentTime` as timestamp ?
              val metadata = Metadata(
                id = id,
                topic = topic,
                deleteTo = 0,
                segmentSize = segmentSize,
                created = timestamp,
                updated = timestamp)
              println(s"$id EventualCassandra.metadata: $metadata")
              for {
                _ <- prepared.insertMetadata(metadata)
              } yield {
                segmentSize
              }
            } else {
              segmentSize(id, prepared)
            }
          }

          for {
            (session, prepared) <- sessionAndPreparedStatements
            segmentSize <- insertMetadata(prepared)
            statementFinal = statement(prepared, segmentSize).set(statementConfig)
            _ <- session.executeAsync(statementFinal).asScala()
          } yield {

          }
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

          val deletedTo = metadata.map { _.deleteTo } getOrElse 0L

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

      // TODO consider merging with save method
      def delete(id: Id, to: SeqNr): Future[Unit] = {

        for {
          (session, statements) <- sessionAndPreparedStatements
          segmentSize <- segmentSize(id, statements)
        } yield {


        }

        ???
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