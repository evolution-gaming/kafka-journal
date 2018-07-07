package com.evolutiongaming.kafka.journal.eventual.cassandra

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Sink, Source}
import com.datastax.driver.core.policies.{LoggingRetryPolicy, RetryPolicy}
import com.datastax.driver.core.{Metadata => _, _}
import com.evolutiongaming.cassandra.Helpers._
import com.evolutiongaming.cassandra.NextHostRetryPolicy
import com.evolutiongaming.kafka.journal.Alias._
import com.evolutiongaming.kafka.journal.FutureHelper._
import com.evolutiongaming.kafka.journal.SeqRange
import com.evolutiongaming.kafka.journal.StreamHelper._
import com.evolutiongaming.kafka.journal.eventual._
import com.evolutiongaming.skafka.Topic

import scala.collection.immutable.{Iterable, Seq}
import scala.concurrent.{ExecutionContext, Future}


// TODO create collection that is optimised for ordered sequence and seqNr
object EventualCassandra {

  case class StatementConfig(
    idempotent: Boolean = false,
    consistencyLevel: ConsistencyLevel,
    retryPolicy: RetryPolicy)


  def apply(
    session: Session,
    schemaConfig: SchemaConfig,
    config: EventualCassandraConfig)(implicit system: ActorSystem, ec: ExecutionContext): Eventual = {

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
      selectLastRecord: JournalStatement.SelectLastRecord.Type,
      selectRecords: JournalStatement.SelectRecords.Type,
      selectMetadata: MetadataStatement.Select.Type,
      selectSegmentSize: MetadataStatement.SelectSegmentSize.Type,
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

      val selectLastRecord = JournalStatement.SelectLastRecord(journalName, prepareAndExecute)
      val listRecords = JournalStatement.SelectRecords(journalName, prepareAndExecute)
      val selectMetadata = MetadataStatement.Select(metadataName, prepareAndExecute)
      val selectSegmentSize = MetadataStatement.SelectSegmentSize(metadataName, prepareAndExecute)
      val updatePointer = PointerStatement.Update(pointerName, prepareAndExecute)
      val selectPointer = PointerStatement.Select(pointerName, prepareAndExecute)
      val selectTopicPointers = PointerStatement.SelectTopicPointers(pointerName, prepareAndExecute)

      for {
        selectLastRecord <- selectLastRecord
        listRecords <- listRecords
        selectMetadata <- selectMetadata
        selectSegmentSize <- selectSegmentSize
        updatePointer <- updatePointer
        selectPointer <- selectPointer
        selectTopicPointers <- selectTopicPointers
      } yield {
        PreparedStatements(
          selectLastRecord,
          listRecords,
          selectMetadata,
          selectSegmentSize,
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


    new Eventual {

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

      // TODO test use case when cassandra is not up to last Action.Truncate

      def list(id: Id, range: SeqRange): Future[Seq[EventualRecord]] = {

        println(s"$id EventualCassandra.list range: $range")

        def list(statement: JournalStatement.SelectRecords.Type, metadata: Metadata) = {

          println(s"$id EventualCassandra.list metadata: $metadata")

          val segmentSize = metadata.segmentSize

          def segmentOf(seqNr: SeqNr) = Segment(seqNr, segmentSize)

          def list(range: SeqRange) = {
            val segment = segmentOf(range.from)
            val source = Source.unfoldWhile(segment) { segment =>
              println(s"$id Source.unfoldWhile range: $range, segment: $segment")
                for {
                  records <- statement(id, segment, range)
                } yield {
                  if (records.isEmpty) {
                    (segment, false, Iterable.empty)
                  } else {
                    val seqNrNext = records.last.seqNr.next
                    val segmentNext = segmentOf(seqNrNext)
                    val continue = (range contains seqNrNext) && segment != segmentNext
                    (segmentNext, continue, records)
                  }
                }
            }

            source.runWith(Sink.seq)
          }

          val deletedTo = metadata.deletedTo

          println(s"$id EventualCassandra.list deletedTo: $deletedTo")

          if(deletedTo >= range.to) {
            Future.seq
          } else {
            val from = range.from max (deletedTo + 1)
            val range2 = range.copy(from = from)
            list(range2)
          }
        }

        for {
          (session, statements) <- sessionAndPreparedStatements
          metadata <- metadata(id, statements)
          result <- metadata match {
            case Some(metadata) => list(statements.selectRecords, metadata)
            case None => Future.seq
          }
        } yield {
          println(s"$id EventualCassandra.list ${ result.map { _.seqNr }.mkString(",") }")
          result
        }
      }

      // TODO remove range argument
      def lastSeqNr(id: Id, from: SeqNr) = {
        // TODO use range.to
        /*def lastSeqNr(statement: JournalStatement.SelectLastRecord.Type, segmentSize: Int) = {

          // TODO create lastSeqNr statement
          // TODO remove duplication
          def recur(from: SeqNr, prev: Option[(Segment, SeqNr)]): Future[Option[SeqNr]] = {
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
                    val seqNr = (segment, result.seqNr)
                    recur(from.next, Some(seqNr))
                }
              } yield {
                result
              }
            }
          }

          recur(range.from, None)
        }*/


        def lastSeqNr(statement: JournalStatement.SelectLastRecord.Type, metadata: Metadata) = {
          LastSeqNr(id, from, statement, metadata)
        }

        for {
          (session, statements) <- sessionAndPreparedStatements
          metadata <- metadata(id, statements)
          seqNr <- metadata match {
            case Some(metadata) => lastSeqNr(statements.selectLastRecord, metadata)
            case None => Future.successful(SeqNr.Min) // TODO cache value
          }
        } yield {
          println(s"$id lastSeqNr: $seqNr")
          Some(seqNr) // TODO simplify api
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

