package com.evolutiongaming.kafka.journal.eventual.cassandra

import java.time.Instant

import cats.data.{NonEmptyList => Nel}
import cats.effect.{Concurrent, Sync, Timer}
import cats.implicits._
import cats.temp.par._
import cats.{Applicative, Monad}
import com.evolutiongaming.catshelper.{FromFuture, LogOf, ToFuture}
import com.evolutiongaming.kafka.journal.CatsHelper._
import com.evolutiongaming.kafka.journal._
import com.evolutiongaming.kafka.journal.eventual.ReplicatedJournal.Metrics
import com.evolutiongaming.kafka.journal.eventual._
import com.evolutiongaming.skafka.Topic
import com.evolutiongaming.smetrics.MeasureDuration

import scala.annotation.tailrec


// TODO test ReplicatedCassandra
object ReplicatedCassandra {

  def of[F[_] : Concurrent : FromFuture : ToFuture : Par : Timer : CassandraCluster : CassandraSession : LogOf : MeasureDuration](
    config: EventualCassandraConfig,
    metrics: Option[Metrics[F]]
  ): F[ReplicatedJournal[F]] = {

    for {
      schema     <- SetupSchema[F](config.schema)
      statements <- Statements.of[F](schema)
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

      def append(key: Key, partitionOffset: PartitionOffset, timestamp: Instant, events: Nel[EventRecord]) = {

        def append(segmentSize: Int) = {

          @tailrec
          def loop(
            events: List[EventRecord],
            s: Option[(Segment, Nel[EventRecord])],
            result: F[Unit]): F[Unit] = {

            def insert(segment: Segment, events: Nel[EventRecord]) = {
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
                    case Some(next) => loop(tail, Some((next, Nel.of(head))), insert(segment, batch))
                  }
                  case None                   => loop(tail, Some((Segment(seqNr, segmentSize), Nel.of(head))), result)
                }

              case Nil => s.fold(result) { case (segment, batch) => insert(segment, batch) }
            }
          }

          loop(events.toList, None, ().pure[F])
        }

        def saveHeadAndSegmentSize(head: Option[Head]) = {
          val seqNrLast = events.last.seqNr

          head.fold {
            val head = Head(
              partitionOffset = partitionOffset,
              segmentSize = segmentSize,
              seqNr = seqNrLast,
              deleteTo = events.head.seqNr.prev)
            val origin = events.head.origin
            val insert = () => Statements[F].insertHead(key, timestamp, head, origin)
            (insert, head.segmentSize)
          } { head =>
            val update = () => Statements[F].updateSeqNr(key, partitionOffset, timestamp, seqNrLast)
            (update, head.segmentSize)
          }
        }

        for {
          head                    <- Statements[F].selectHead(key)
          (saveHead, segmentSize)  = saveHeadAndSegmentSize(head)
          _                       <- Sync[F].uncancelable {
            for {
              _ <- append(segmentSize)
              _ <- saveHead()
            } yield {}
          }
        } yield {}
      }


      def delete(key: Key, partitionOffset: PartitionOffset, timestamp: Instant, deleteTo: SeqNr, origin: Option[Origin]) = {

        def saveHead(head: Option[Head]) = {
          head.fold {
            val head = Head(
              partitionOffset = partitionOffset,
              segmentSize = segmentSize,
              seqNr = deleteTo,
              deleteTo = Some(deleteTo))
            for {
              _ <- Statements[F].insertHead(key, timestamp, head, origin)
            } yield head.segmentSize
          } { head =>
            val update =
              if (head.seqNr >= deleteTo) {
                Statements[F].updateDeleteTo(key, partitionOffset, timestamp, deleteTo)
              } else {
                Statements[F].updateHead(key, partitionOffset, timestamp, deleteTo, deleteTo)
              }
            for {
              _ <- update
            } yield head.segmentSize
          }
        }

        def delete(segmentSize: Int, head: Head) = {

          def delete(from: SeqNr, deleteTo: SeqNr) = {

            def segment(seqNr: SeqNr) = SegmentNr(seqNr, segmentSize)

            (segment(from) to segment(deleteTo)).parFoldMap { segment =>
              Statements[F].deleteRecords(key, segment, deleteTo)
            }
          }

          val deleteToFixed = head.seqNr min deleteTo

          head.deleteTo match {
            case None           => delete(from = SeqNr.Min, deleteTo = deleteToFixed)
            case Some(deleteTo) =>
              if (deleteTo >= deleteToFixed) ().pure[F]
              else deleteTo.next.foldMap { from => delete(from = from, deleteTo = deleteToFixed) }
          }
        }

        for {
          head <- Statements[F].selectHead(key)
          _    <- Sync[F].uncancelable {
            for {
              segmentSize <- saveHead(head)
              _           <- head.foldMap[F[Unit]](delete(segmentSize, _))
            } yield {}
          }
        } yield {}
      }


      def save(topic: Topic, topicPointers: TopicPointers, timestamp: Instant) = {
        topicPointers.values.toList.parFoldMap { case (partition, offset) =>
          val insert = PointerInsert(
            topic = topic,
            partition = partition,
            offset = offset,
            updated = timestamp,
            created = timestamp)
          Statements[F].insertPointer(insert)
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
    insertHead    : HeadStatement.Insert[F],
    selectHead    : HeadStatement.Select[F],
    updateHead    : HeadStatement.Update[F],
    updateSeqNr   : HeadStatement.UpdateSeqNr[F],
    updateDeleteTo: HeadStatement.UpdateDeleteTo[F],
    insertPointer : PointerStatement.Insert[F],
    selectPointers: PointerStatement.SelectPointers[F],
    selectTopics  : PointerStatement.SelectTopics[F])

  object Statements {

    def apply[F[_]](implicit F: Statements[F]): Statements[F] = F

    def of[F[_] : Monad : Par : CassandraSession](schema: Schema): F[Statements[F]] = {
      val statements = (
        JournalStatement.InsertRecords.of[F](schema.journal),
        JournalStatement.DeleteRecords.of[F](schema.journal),
        HeadStatement.Insert.of[F](schema.head),
        HeadStatement.Select.of[F](schema.head),
        HeadStatement.Update.of[F](schema.head),
        HeadStatement.UpdateSeqNr.of[F](schema.head),
        HeadStatement.UpdateDeleteTo.of[F](schema.head),
        PointerStatement.Insert.of[F](schema.pointer),
        PointerStatement.SelectPointers.of[F](schema.pointer),
        PointerStatement.SelectTopics.of[F](schema.pointer))
      statements.parMapN(Statements[F])
    }
  }
}