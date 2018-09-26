package com.evolutiongaming.kafka.journal.eventual.cassandra

import java.time.Instant

import com.evolutiongaming.cassandra.Session
import com.evolutiongaming.concurrent.async.Async
import com.evolutiongaming.kafka.journal._
import com.evolutiongaming.kafka.journal.eventual._
import com.evolutiongaming.nel.Nel
import com.evolutiongaming.skafka.Topic

import scala.annotation.tailrec
import scala.concurrent.ExecutionContext


// TODO create collection that is optimised for ordered sequence and seqNr
// TODO redesign EventualDbCassandra so it can hold stat and called recursively
// TODO add logs to ReplicatedCassandra
object ReplicatedCassandra {

  def apply(session: Session, config: EventualCassandraConfig)
    (implicit ec: ExecutionContext): ReplicatedJournal[Async] = {

    val statements = for {
      tables <- CreateSchema(config.schema, session)
      prepareAndExecute = PrepareAndExecute(session, config.retries)
      statements <- Statements(tables, prepareAndExecute)
    } yield {
      statements
    }
    apply(statements, config.segmentSize)
  }


  def apply(
    statements: Async[Statements],
    segmentSize: Int): ReplicatedJournal[Async] = new ReplicatedJournal[Async] {

    def topics() = {
      for {
        statements <- statements
        topics <- statements.selectTopics()
      } yield topics.sorted
    }

    def append(key: Key, partitionOffset: PartitionOffset, timestamp: Instant, events: Nel[ReplicatedEvent]) = {

      def append(statements: Statements, segmentSize: Int) = {

        @tailrec
        def loop(
          events: List[ReplicatedEvent],
          s: Option[(Segment, Nel[ReplicatedEvent])], // TODO not tuple
          result: Async[Unit]): Async[Unit] = {

          def execute(segment: Segment, events: Nel[ReplicatedEvent]) = {
            val next = statements.insertRecords(key, segment.nr, events)
            for {
              _ <- result
              _ <- next
            } yield {}
          }

          events match {
            case head :: tail =>
              val seqNr = head.event.seqNr
              s match {
                case Some((segment, batch)) => segment.next(seqNr) match {
                  case None       => loop(tail, Some((segment, head :: batch)), result)
                  case Some(next) => loop(tail, Some((next, Nel(head))), execute(segment, batch))
                }
                case None                   => loop(tail, Some((Segment(seqNr, segmentSize), Nel(head))), result)
              }

            case Nil => s.fold(result) { case (segment, batch) => execute(segment, batch) }
          }
        }

        loop(events.toList, None, Async.unit)
      }

      def saveMetadataAndSegmentSize(statements: Statements, metadata: Option[Metadata]) = {
        val seqNr = events.last.seqNr

        metadata match {
          case Some(metadata) =>
            val update = () => statements.updateSeqNr(key, partitionOffset, timestamp, seqNr)
            (update, metadata.segmentSize)

          case None =>
            val metadata = Metadata(partitionOffset = partitionOffset, segmentSize = segmentSize, seqNr = seqNr, deleteTo = None)
            val origin = events.head.origin
            val insert = () => statements.insertMetadata(key, timestamp, metadata, origin)
            (insert, metadata.segmentSize)
        }
      }

      for {
        statements <- statements
        metadata <- statements.selectMetadata(key)
        (saveMetadata, segmentSize) = saveMetadataAndSegmentSize(statements, metadata)
        _ <- append(statements, segmentSize)
        _ <- saveMetadata()
      } yield {}
    }


    def delete(key: Key, partitionOffset: PartitionOffset, timestamp: Instant, deleteTo: SeqNr, origin: Option[Origin]) = {

      def saveMetadata(statements: Statements, metadata: Option[Metadata]) = {
        metadata match {
          case Some(metadata) =>
            val update =
              if (metadata.seqNr >= deleteTo) {
                statements.updateDeleteTo(key, partitionOffset, timestamp, deleteTo)
              } else {
                statements.updateMetadata(key, partitionOffset, timestamp, deleteTo, deleteTo)
              }
            for {
              _ <- update
            } yield metadata.segmentSize

          case None =>
            val metadata = Metadata(
              partitionOffset = partitionOffset,
              segmentSize = segmentSize,
              seqNr = deleteTo,
              deleteTo = Some(deleteTo))
            for {
              _ <- statements.insertMetadata(key, timestamp, metadata, origin)
            } yield metadata.segmentSize
        }
      }

      def delete(statements: Statements, segmentSize: Int, metadata: Metadata) = {

        def delete(from: SeqNr, deleteTo: SeqNr) = {

          def segment(seqNr: SeqNr) = SegmentNr(seqNr, segmentSize)

          Async.foldUnit {
            for {
              segment <- segment(from) to segment(deleteTo) // TODO maybe add ability to create Seq[Segment] out of SeqRange ?
            } yield {
              statements.deleteRecords(key, segment, deleteTo)
            }
          }
        }

        val deleteToFixed = metadata.seqNr min deleteTo

        metadata.deleteTo match {
          case None            => delete(from = SeqNr.Min, deleteTo = deleteToFixed)
          case Some(deletedTo) =>
            if (deletedTo >= deleteToFixed) Async.unit
            else deletedTo.next match {
              case None       => Async.unit
              case Some(from) => delete(from = from, deleteTo = deleteToFixed)
            }
        }
      }

      for {
        statements <- statements
        metadata <- statements.selectMetadata(key)
        segmentSize <- saveMetadata(statements, metadata)
        _ <- metadata.fold(Async.unit) { delete(statements, segmentSize, _) }
      } yield {}
    }


    def save(topic: Topic, topicPointers: TopicPointers, timestamp: Instant): Async[Unit] = {
      val pointers = topicPointers.values
      if (pointers.isEmpty) Async.unit
      else {

        // TODO topic is a partition key, should I batch by partition ?

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
        } yield {}
      }
    }

    def pointers(topic: Topic) = {
      for {
        statements <- statements
        topicPointers <- statements.selectPointers(topic)
      } yield topicPointers
    }
  }


  final case class Statements(
    insertRecords: JournalStatement.InsertRecords.Type,
    selectLastRecord: JournalStatement.SelectLastRecord.Type[Async], // TODO not used
    deleteRecords: JournalStatement.DeleteRecords.Type,
    insertMetadata: MetadataStatement.Insert.Type,
    selectMetadata: MetadataStatement.Select.Type,
    updateMetadata: MetadataStatement.Update.Type,
    updateSeqNr: MetadataStatement.UpdateSeqNr.Type,
    updateDeleteTo: MetadataStatement.UpdateDeleteTo.Type,
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
      val updateSeqNr = MetadataStatement.UpdateSeqNr(tables.metadata, prepareAndExecute)
      val updateDeleteTo = MetadataStatement.UpdateDeleteTo(tables.metadata, prepareAndExecute)
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
        updateSeqNr <- updateSeqNr
        updateDeleteTo <- updateDeleteTo
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
          updateSeqNr = updateSeqNr,
          updateDeleteTo = updateDeleteTo,
          insertPointer = insertPointer,
          selectPointers = selectPointers,
          selectTopics = selectTopics)
      }
    }
  }
}