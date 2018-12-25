package com.evolutiongaming.kafka.journal.eventual.cassandra

import java.time.Instant

import cats.implicits._
import cats.effect.IO
import cats.{Applicative, FlatMap, Monad}
import com.evolutiongaming.concurrent.async.Async
import com.evolutiongaming.concurrent.async.AsyncConverters._
import com.evolutiongaming.kafka.journal._
import com.evolutiongaming.kafka.journal.eventual._
import com.evolutiongaming.kafka.journal.util.{FromFuture, Par}
import com.evolutiongaming.kafka.journal.util.CatsHelper._
import com.evolutiongaming.nel.Nel
import com.evolutiongaming.scassandra.Session
import com.evolutiongaming.skafka.Topic

import scala.annotation.tailrec
import scala.concurrent.ExecutionContext
import scala.concurrent.duration._


// TODO redesign EventualDbCassandra so it can hold state and called recursively
// TODO test ReplicatedCassandra
object ReplicatedCassandra {

  def apply(
    config: EventualCassandraConfig)(implicit
    ec: ExecutionContext,
    session: Session): ReplicatedJournal[Async] = {

    async(config).get(30.seconds) // TODO
  }

  def async(
    config: EventualCassandraConfig)(implicit
    ec: ExecutionContext,
    session: Session): Async[ReplicatedJournal[Async]] = {

    implicit val cs = IO.contextShift(ec)
    implicit val fromFuture = FromFuture.io
    implicit val cassandraSession = CassandraSession(CassandraSession[IO](session), config.retries)
    implicit val cassandraSync = CassandraSync[IO](config.schema, Some(Origin("replicator")))

    val journal = for {
      journal <- of[IO](config)
    } yield {
      new ReplicatedJournal[Async] {

        def topics = journal.topics.unsafeToFuture().async

        def pointers(topic: Topic) = {
          journal.pointers(topic).unsafeToFuture().async
        }

        def append(key: Key, partitionOffset: PartitionOffset, timestamp: Instant, events: Nel[ReplicatedEvent]) = {
          journal.append(key, partitionOffset, timestamp, events).unsafeToFuture().async
        }

        def delete(key: Key, partitionOffset: PartitionOffset, timestamp: Instant, deleteTo: SeqNr, origin: Option[Origin]) = {
          journal.delete(key, partitionOffset, timestamp, deleteTo, origin).unsafeToFuture().async
        }

        def save(topic: Topic, pointers: TopicPointers, timestamp: Instant) = {
          journal.save(topic, pointers, timestamp).unsafeToFuture().async
        }
      }
    }

    Async(journal.unsafeToFuture())
  }


  def of[F[_] : Monad : Par : CassandraSession : CassandraSync](config: EventualCassandraConfig): F[ReplicatedJournal[F]] = {
    for {
      tables     <- CreateSchema[F](config.schema)
      statements <- Statements.of[F](tables)
    } yield {
      implicit val statements1 = statements
      apply(config.segmentSize)
    }
  }


  def apply[F[_] : Monad : Par : Statements](segmentSize: Int): ReplicatedJournal[F] = {

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
          _                           <- append(segmentSize)
          _                           <- saveMetadata()
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

            Par[F].unorderedFold {
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
          segmentSize <- saveMetadata(metadata)
          _           <- metadata.foldMap[F[Unit]](delete(segmentSize, _))
        } yield {}
      }


      def save(topic: Topic, topicPointers: TopicPointers, timestamp: Instant) = {
        Par[F].unorderedFold {
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
    insertRecords : JournalStatement.InsertRecords.Type[F],
    deleteRecords : JournalStatement.DeleteRecords.Type[F],
    insertMetadata: MetadataStatement.Insert.Type[F],
    selectMetadata: MetadataStatement.Select.Type[F],
    updateMetadata: MetadataStatement.Update.Type[F],
    updateSeqNr   : MetadataStatement.UpdateSeqNr.Type[F],
    updateDeleteTo: MetadataStatement.UpdateDeleteTo.Type[F],
    insertPointer : PointerStatement.Insert.Type[F],
    selectPointers: PointerStatement.SelectPointers.Type[F],
    selectTopics  : PointerStatement.SelectTopics.Type[F])

  object Statements {

    def apply[F[_]](implicit F: Statements[F]): Statements[F] = F

    def of[F[_] : FlatMap : Par : CassandraSession](tables: Tables): F[Statements[F]] = {
      val statements = (
        JournalStatement.InsertRecords[F](tables.journal),
        JournalStatement.DeleteRecords[F](tables.journal),
        MetadataStatement.Insert[F](tables.metadata),
        MetadataStatement.Select[F](tables.metadata),
        MetadataStatement.Update[F](tables.metadata),
        MetadataStatement.UpdateSeqNr[F](tables.metadata),
        MetadataStatement.UpdateDeleteTo[F](tables.metadata),
        PointerStatement.Insert[F](tables.pointer),
        PointerStatement.SelectPointers[F](tables.pointer),
        PointerStatement.SelectTopics[F](tables.pointer))
      Par[F].mapN(statements)(Statements[F])
    }
  }
}