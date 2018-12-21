package com.evolutiongaming.kafka.journal.eventual.cassandra

import java.time.Instant

import cats.FlatMap
import com.evolutiongaming.concurrent.async.Async
import com.evolutiongaming.kafka.journal._
import com.evolutiongaming.kafka.journal.eventual._
import com.evolutiongaming.nel.Nel
import com.evolutiongaming.scassandra.Session
import com.evolutiongaming.skafka.Topic

import scala.annotation.tailrec
import scala.concurrent.ExecutionContext


// TODO create collection that is optimised for ordered sequence and seqNr
// TODO redesign EventualDbCassandra so it can hold stat and called recursively
// TODO add logs to ReplicatedCassandra
object ReplicatedCassandra {

  def apply(
    config: EventualCassandraConfig)(implicit
    ec: ExecutionContext,
    session: Session): ReplicatedJournal[Async] = {

    import com.evolutiongaming.kafka.journal.AsyncImplicits._

    implicit val cassandraSession = CassandraSession(CassandraSession[Async](session), config.retries)
    implicit val cassandraSync = CassandraSync(config.schema, Some(Origin("replicator")))
    val statements = for {
      tables <- CreateSchema(config.schema)
      statements <- Statements(tables)
    } yield {
      statements
    }
    apply(statements, config.segmentSize)
  }


  def apply[F[_] : IO2](
    statements: F[Statements[F]],
    segmentSize: Int): ReplicatedJournal[F] = new ReplicatedJournal[F] {

    import com.evolutiongaming.kafka.journal.IO2.ops._

    def topics = {
      for {
        statements <- statements
        topics <- statements.selectTopics()
      } yield topics.sorted
    }

    def append(key: Key, partitionOffset: PartitionOffset, timestamp: Instant, events: Nel[ReplicatedEvent]) = {

      def append(statements: Statements[F], segmentSize: Int) = {

        @tailrec
        def loop(
          events: List[ReplicatedEvent],
          s: Option[(Segment, Nel[ReplicatedEvent])], // TODO not tuple
          result: F[Unit]): F[Unit] = {

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

        loop(events.toList, None, IO2[F].unit)
      }

      def saveMetadataAndSegmentSize(statements: Statements[F], metadata: Option[Metadata]) = {
        val seqNrLast = events.last.seqNr

        metadata match {
          case Some(metadata) =>
            val update = () => statements.updateSeqNr(key, partitionOffset, timestamp, seqNrLast)
            (update, metadata.segmentSize)

          case None =>
            val metadata = Metadata(
              partitionOffset = partitionOffset,
              segmentSize = segmentSize,
              seqNr = seqNrLast,
              deleteTo = events.head.seqNr.prev)
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

      def saveMetadata(statements: Statements[F], metadata: Option[Metadata]) = {
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

      def delete(statements: Statements[F], segmentSize: Int, metadata: Metadata) = {

        def delete(from: SeqNr, deleteTo: SeqNr) = {

          def segment(seqNr: SeqNr) = SegmentNr(seqNr, segmentSize)

          IO2[F].foldUnit {
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
            if (deletedTo >= deleteToFixed) IO2[F].unit
            else deletedTo.next match {
              case None       => IO2[F].unit
              case Some(from) => delete(from = from, deleteTo = deleteToFixed)
            }
        }
      }

      for {
        statements <- statements
        metadata <- statements.selectMetadata(key)
        segmentSize <- saveMetadata(statements, metadata)
        _ <- metadata.fold(IO2[F].unit) { delete(statements, segmentSize, _) }
      } yield {}
    }


    def save(topic: Topic, topicPointers: TopicPointers, timestamp: Instant) = {
      val pointers = topicPointers.values
      if (pointers.isEmpty) IO2[F].unit
      else {

        // TODO topic is a partition key, should I batch by partition ?

        def savePointers(statements: Statements[F]) = {
          val results = for {
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

          IO2[F].foldUnit(results)
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


  final case class Statements[F[_]](
    insertRecords: JournalStatement.InsertRecords.Type[F],
    deleteRecords: JournalStatement.DeleteRecords.Type[F],
    insertMetadata: MetadataStatement.Insert.Type[F],
    selectMetadata: MetadataStatement.Select.Type[F],
    updateMetadata: MetadataStatement.Update.Type[F],
    updateSeqNr: MetadataStatement.UpdateSeqNr.Type[F],
    updateDeleteTo: MetadataStatement.UpdateDeleteTo.Type[F],
    insertPointer: PointerStatement.Insert.Type[F],
    selectPointers: PointerStatement.SelectPointers.Type[F],
    selectTopics: PointerStatement.SelectTopics.Type[F])

  object Statements {

    import cats.implicits._

    def apply[F[_] : FlatMap : CassandraSession](tables: Tables): F[Statements[F]] = {

      val insertRecords  = JournalStatement.InsertRecords[F](tables.journal)
      val deleteRecords  = JournalStatement.DeleteRecords[F](tables.journal)
      val insertMetadata = MetadataStatement.Insert[F](tables.metadata)
      val selectMetadata = MetadataStatement.Select[F](tables.metadata)
      val updateMetadata = MetadataStatement.Update[F](tables.metadata)
      val updateSeqNr    = MetadataStatement.UpdateSeqNr[F](tables.metadata)
      val updateDeleteTo = MetadataStatement.UpdateDeleteTo[F](tables.metadata)
      val insertPointer  = PointerStatement.Insert[F](tables.pointer)
      val selectPointers = PointerStatement.SelectPointers[F](tables.pointer)
      val selectTopics   = PointerStatement.SelectTopics[F](tables.pointer)

      for {
        insertRecords  <- insertRecords
        deleteRecords  <- deleteRecords
        insertMetadata <- insertMetadata
        selectMetadata <- selectMetadata
        updateMetadata <- updateMetadata
        updateSeqNr    <- updateSeqNr
        updateDeleteTo <- updateDeleteTo
        insertPointer  <- insertPointer
        selectPointers <- selectPointers
        selectTopics   <- selectTopics
      } yield {
        Statements(
          insertRecords  = insertRecords,
          deleteRecords  = deleteRecords,
          insertMetadata = insertMetadata,
          selectMetadata = selectMetadata,
          updateMetadata = updateMetadata,
          updateSeqNr    = updateSeqNr,
          updateDeleteTo = updateDeleteTo,
          insertPointer  = insertPointer,
          selectPointers = selectPointers,
          selectTopics   = selectTopics)
      }
    }
  }
}