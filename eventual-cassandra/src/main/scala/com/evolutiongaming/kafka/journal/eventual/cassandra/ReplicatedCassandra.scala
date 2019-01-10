package com.evolutiongaming.kafka.journal.eventual.cassandra

import java.time.Instant

import cats.effect.{Clock, Concurrent, Sync}
import cats.implicits._
import cats.{Applicative, FlatMap}
import com.evolutiongaming.kafka.journal._
import com.evolutiongaming.kafka.journal.eventual.ReplicatedJournal.Metrics
import com.evolutiongaming.kafka.journal.eventual._
import com.evolutiongaming.kafka.journal.util.CatsHelper._
import com.evolutiongaming.kafka.journal.util.{FromFuture, Par, ToFuture}
import com.evolutiongaming.nel.Nel
import com.evolutiongaming.skafka.Topic

import scala.annotation.tailrec


// TODO redesign EventualDbCassandra so it can hold state and called recursively
// TODO test ReplicatedCassandra
object ReplicatedCassandra {

  def of[F[_] : Concurrent : FromFuture : ToFuture : Par : Clock : CassandraSession : LogOf](
    config: EventualCassandraConfig,
    metrics: Option[Metrics[F]]): F[ReplicatedJournal[F]] = {

    implicit val cassandraSync = CassandraSync[F](config.schema, Some(Origin("replicator"/*TODO*/)))
    for {
      tables     <- CreateSchema[F](config.schema)
      statements <- Statements.of[F](tables)
      log        <- LogOf[F].apply(ReplicatedCassandra.getClass)
    } yield {
      implicit val statements1 = statements
      val journal = apply[F](config.segmentSize)
      ReplicatedJournal[F](journal, log, metrics)
    }
  }

  def apply[F[_] : Sync : Par : Statements](segmentSize: Int): ReplicatedJournal[F] = {

    implicit val monoidUnit = Applicative.monoid[F, Unit]

    new ReplicatedJournal[F] {

      def topics = {
        for {
          topics <- Statements[F].selectTopics()
        } yield topics.sorted
      }

      def append(key: Key, partitionOffset: PartitionOffset, timestamp: Instant, events: Nel[ReplicatedEvent]) = {

        def append(segmentSize: Int) = {

          @tailrec
          def loop(
            events: List[ReplicatedEvent],
            s: Option[(Segment, Nel[ReplicatedEvent])],
            result: F[Unit]): F[Unit] = {

            def insert(segment: Segment, events: Nel[ReplicatedEvent]) = {
              val next = Statements[F].insertRecords(key, segment.nr, events)
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
                    case Some(next) => loop(tail, Some((next, Nel(head))), insert(segment, batch))
                  }
                  case None                   => loop(tail, Some((Segment(seqNr, segmentSize), Nel(head))), result)
                }

              case Nil => s.fold(result) { case (segment, batch) => insert(segment, batch) }
            }
          }

          loop(events.toList, None, ().pure[F])
        }

        def saveMetadataAndSegmentSize(metadata: Option[Metadata]) = {
          val seqNrLast = events.last.seqNr

          metadata match {
            case Some(metadata) =>
              val update = () => Statements[F].updateSeqNr(key, partitionOffset, timestamp, seqNrLast)
              (update, metadata.segmentSize)

            case None =>
              val metadata = Metadata(
                partitionOffset = partitionOffset,
                segmentSize = segmentSize,
                seqNr = seqNrLast,
                deleteTo = events.head.seqNr.prev)
              val origin = events.head.origin
              val insert = () => Statements[F].insertMetadata(key, timestamp, metadata, origin)
              (insert, metadata.segmentSize)
          }
        }

        for {
          metadata                    <- Statements[F].selectMetadata(key)
          (saveMetadata, segmentSize)  = saveMetadataAndSegmentSize(metadata)
          _                           <- Sync[F].uncancelable {
            for {
              _ <- append(segmentSize)
              _ <- saveMetadata()
            } yield {}
          }
        } yield {}
      }


      def delete(key: Key, partitionOffset: PartitionOffset, timestamp: Instant, deleteTo: SeqNr, origin: Option[Origin]) = {

        def saveMetadata(metadata: Option[Metadata]) = {
          metadata.fold {
            val metadata = Metadata(
              partitionOffset = partitionOffset,
              segmentSize = segmentSize,
              seqNr = deleteTo,
              deleteTo = Some(deleteTo))
            for {
              _ <- Statements[F].insertMetadata(key, timestamp, metadata, origin)
            } yield metadata.segmentSize
          } { metadata =>
            val update =
              if (metadata.seqNr >= deleteTo) {
                Statements[F].updateDeleteTo(key, partitionOffset, timestamp, deleteTo)
              } else {
                Statements[F].updateMetadata(key, partitionOffset, timestamp, deleteTo, deleteTo)
              }
            for {
              _ <- update
            } yield metadata.segmentSize
          }
        }

        def delete(segmentSize: Int, metadata: Metadata) = {

          def delete(from: SeqNr, deleteTo: SeqNr) = {

            def segment(seqNr: SeqNr) = SegmentNr(seqNr, segmentSize)

            Par[F].fold {
              for {
                segment <- segment(from) to segment(deleteTo) // TODO maybe add ability to create Seq[Segment] out of SeqRange ?
              } yield {
                Statements[F].deleteRecords(key, segment, deleteTo)
              }
            }
          }

          val deleteToFixed = metadata.seqNr min deleteTo

          metadata.deleteTo match {
            case None            => delete(from = SeqNr.Min, deleteTo = deleteToFixed)
            case Some(deletedTo) =>
              if (deletedTo >= deleteToFixed) ().pure[F]
              else deletedTo.next.foldMap { from => delete(from = from, deleteTo = deleteToFixed) }
          }
        }

        for {
          metadata    <- Statements[F].selectMetadata(key)
          _           <- Sync[F].uncancelable {
            for {
              segmentSize <- saveMetadata(metadata)
              _           <- metadata.foldMap[F[Unit]](delete(segmentSize, _))
            } yield {}
          }
        } yield {}
      }


      def save(topic: Topic, topicPointers: TopicPointers, timestamp: Instant) = {
        Par[F].fold {
          for {
            (partition, offset) <- topicPointers.values
          } yield {
            val insert = PointerInsert(
              topic = topic,
              partition = partition,
              offset = offset,
              updated = timestamp,
              created = timestamp)
            Statements[F].insertPointer(insert)
          }
        }
      }

      def pointers(topic: Topic) = {
        Statements[F].selectPointers(topic)
      }
    }
  }


  final case class Statements[F[_]](
    insertRecords : JournalStatement.InsertRecords[F],
    deleteRecords : JournalStatement.DeleteRecords[F],
    insertMetadata: MetadataStatement.Insert[F],
    selectMetadata: MetadataStatement.Select[F],
    updateMetadata: MetadataStatement.Update[F],
    updateSeqNr   : MetadataStatement.UpdateSeqNr[F],
    updateDeleteTo: MetadataStatement.UpdateDeleteTo[F],
    insertPointer : PointerStatement.Insert[F],
    selectPointers: PointerStatement.SelectPointers[F],
    selectTopics  : PointerStatement.SelectTopics[F])

  object Statements {

    def apply[F[_]](implicit F: Statements[F]): Statements[F] = F

    def of[F[_] : FlatMap : Par : CassandraSession](tables: Tables): F[Statements[F]] = {
      val statements = (
        JournalStatement.InsertRecords.of[F](tables.journal),
        JournalStatement.DeleteRecords.of[F](tables.journal),
        MetadataStatement.Insert.of[F](tables.metadata),
        MetadataStatement.Select.of[F](tables.metadata),
        MetadataStatement.Update.of[F](tables.metadata),
        MetadataStatement.UpdateSeqNr.of[F](tables.metadata),
        MetadataStatement.UpdateDeleteTo.of[F](tables.metadata),
        PointerStatement.Insert.of[F](tables.pointer),
        PointerStatement.SelectPointers.of[F](tables.pointer),
        PointerStatement.SelectTopics.of[F](tables.pointer))
      Par[F].mapN(statements)(Statements[F])
    }
  }
}