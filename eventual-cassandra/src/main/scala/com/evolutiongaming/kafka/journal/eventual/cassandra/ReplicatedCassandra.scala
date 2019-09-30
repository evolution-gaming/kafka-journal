package com.evolutiongaming.kafka.journal.eventual.cassandra

import java.time.Instant

import cats.data.{NonEmptyList => Nel}
import cats.effect.{Concurrent, Sync, Timer}
import cats.effect.implicits._
import cats.implicits._
import cats.{Applicative, Monad, Parallel}
import com.evolutiongaming.catshelper.ParallelHelper._
import com.evolutiongaming.catshelper.{FromFuture, LogOf, ToFuture}
import com.evolutiongaming.kafka.journal._
import com.evolutiongaming.kafka.journal.eventual.ReplicatedJournal.Metrics
import com.evolutiongaming.kafka.journal.eventual.{ReplicatedJournal, _}
import com.evolutiongaming.kafka.journal.util.OptionHelper._
import com.evolutiongaming.skafka.Topic
import com.evolutiongaming.smetrics.MeasureDuration

import scala.annotation.tailrec


// TODO test ReplicatedCassandra
object ReplicatedCassandra {

  def of[F[_] : Concurrent : FromFuture : ToFuture : Parallel : Timer : CassandraCluster : CassandraSession : LogOf : MeasureDuration](
    config: EventualCassandraConfig,
    origin: Option[Origin],
    metrics: Option[Metrics[F]]
  ): F[ReplicatedJournal[F]] = {

    for {
      schema     <- SetupSchema[F](config.schema, origin)
      statements <- Statements.of[F](schema)
      log        <- LogOf[F].apply(ReplicatedCassandra.getClass)
    } yield {
      val journal = apply[F](config.segmentSize, statements)
      val journal1 = journal.withLog(log)
      metrics.fold(journal1) { metrics => journal1.withMetrics(metrics) }
    }
  }

  def apply[F[_] : Sync : Parallel](segmentSize: Int, statements: Statements[F]): ReplicatedJournal[F] = {

    implicit val monoidUnit = Applicative.monoid[F, Unit]

    new ReplicatedJournal[F] {

      def topics = {
        for {
          topics <- statements.selectTopics()
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
              deleteTo = events.head.seqNr.prev[Option])
            val origin = events.head.origin
            val insert = () => statements.insertHead(key, timestamp, head, origin)
            (insert, head.segmentSize)
          } { head =>
            val update = () => statements.updateSeqNr(key, partitionOffset, timestamp, seqNrLast)
            (update, head.segmentSize)
          }
        }

        for {
          head                    <- statements.selectHead(key)
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

        def delete(head: Option[Head]) = {

          def saveHead = {
            head.fold {
              val head = Head(
                partitionOffset = partitionOffset,
                segmentSize = segmentSize,
                seqNr = deleteTo,
                deleteTo = Some(deleteTo))
              for {
                _ <- statements.insertHead(key, timestamp, head, origin)
              } yield head.segmentSize
            } { head =>
              val update =
                if (head.seqNr >= deleteTo) {
                  statements.updateDeleteTo(key, partitionOffset, timestamp, deleteTo)
                } else {
                  statements.updateHead(key, partitionOffset, timestamp, deleteTo, deleteTo)
                }
              for {
                _ <- update
              } yield head.segmentSize
            }
          }

          def delete(segmentSize: Int)(head: Head) = {

            def delete(from: SeqNr, deleteTo: SeqNr) = {

              def segment(seqNr: SeqNr) = SegmentNr(seqNr, segmentSize)

              (segment(from) to segment(deleteTo)).parFoldMap { segment =>
                statements.deleteRecords(key, segment, deleteTo)
              }
            }

            val deleteToFixed = head.seqNr min deleteTo

            head.deleteTo.fold {
              delete(from = SeqNr.min, deleteTo = deleteToFixed)
            } { deleteTo =>
              if (deleteTo >= deleteToFixed) ().pure[F]
              else deleteTo.next[Option].foldMap { from => delete(from = from, deleteTo = deleteToFixed) }
            }
          }

          for {
            segmentSize <- saveHead
            _           <- head.foldMap[F[Unit]](delete(segmentSize))
          } yield {}
        }

        for {
          head   <- statements.selectHead(key)
          result <- delete(head).uncancelable
        } yield result
      }


      def save(topic: Topic, topicPointers: TopicPointers, timestamp: Instant) = {
        topicPointers.values.toList.parFoldMap { case (partition, offset) =>
          val insert = PointerInsert(
            topic = topic,
            partition = partition,
            offset = offset,
            updated = timestamp,
            created = timestamp)
          statements.insertPointer(insert)
        }
      }

      def pointers(topic: Topic) = {
        statements.selectPointers(topic)
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

    def of[F[_] : Monad : Parallel : CassandraSession](schema: Schema): F[Statements[F]] = {
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