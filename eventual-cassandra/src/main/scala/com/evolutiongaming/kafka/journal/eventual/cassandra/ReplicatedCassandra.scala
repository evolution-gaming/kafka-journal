package com.evolutiongaming.kafka.journal.eventual.cassandra

import java.time.Instant

import cats.data.{NonEmptyList => Nel}
import cats.effect.implicits._
import cats.effect.{Concurrent, Timer}
import cats.implicits._
import cats.{Applicative, Monad, Parallel}
import com.evolutiongaming.catshelper.ParallelHelper._
import com.evolutiongaming.catshelper.{BracketThrowable, FromFuture, LogOf, ToFuture}
import com.evolutiongaming.kafka.journal._
import com.evolutiongaming.kafka.journal.eventual.ReplicatedJournal.Metrics
import com.evolutiongaming.kafka.journal.eventual.{ReplicatedJournal, _}
import com.evolutiongaming.kafka.journal.util.OptionHelper._
import com.evolutiongaming.skafka.{Offset, Partition, Topic}
import com.evolutiongaming.smetrics.MeasureDuration

import scala.annotation.tailrec


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
        .withLog(log)
      metrics
        .fold(journal) { metrics => journal.withMetrics(metrics) }
        .enhanceError
    }
  }

  def apply[F[_] : BracketThrowable : Parallel](
    segmentSize: SegmentSize,
    statements: Statements[F]
  ): ReplicatedJournal[F] = {

    implicit val monoidUnit = Applicative.monoid[F, Unit]

    new ReplicatedJournal[F] {

      def topics = {
        for {
          topics <- statements.selectTopics()
        } yield topics.sorted
      }

      def append(
        key: Key,
        partitionOffset: PartitionOffset,
        timestamp: Instant,
        events: Nel[EventRecord]
      ) = {

        def append(segmentSize: SegmentSize) = {

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
                  case Some((segment, batch)) => segment.nextUnsafe(seqNr) match {
                    case None       => loop(tail, (segment, head :: batch).some, result)
                    case Some(next) => loop(tail, (next, Nel.of(head)).some, insert(segment, batch))
                  }
                  case None                   => loop(tail, (Segment.unsafe(seqNr, segmentSize), Nel.of(head)).some, result)
                }

              case Nil => s.fold(result) { case (segment, batch) => insert(segment, batch) }
            }
          }

          loop(events.toList, None, ().pure[F])
        }

        def appendAndSave(head: Option[JournalHead]) = {
          val seqNrLast = events.last.seqNr

          val (save, head1) = head.fold {
            val head = JournalHead(
              partitionOffset = partitionOffset,
              segmentSize = segmentSize,
              seqNr = seqNrLast,
              deleteTo = events.head.seqNr.prev[Option])
            val origin = events.head.origin
            val insert = statements.insertMetadata(key, timestamp, head, origin)
            (insert, head)
          } { head =>
            val update = statements.updateSeqNr(key, partitionOffset, timestamp, seqNrLast)
            (update, head)
          }

          for {
            _ <- append(head1.segmentSize)
            _ <- save
          } yield {}
        }

        for {
          head <- statements.selectMetadata(key)
          _    <- appendAndSave(head).uncancelable
        } yield {}
      }


      def delete(
        key: Key,
        partitionOffset: PartitionOffset,
        timestamp: Instant,
        deleteTo: SeqNr,
        origin: Option[Origin]
      ) = {

        def delete(head: Option[JournalHead]) = {

          def insert = {
            val head = JournalHead(
              partitionOffset = partitionOffset,
              segmentSize = segmentSize,
              seqNr = deleteTo,
              deleteTo = deleteTo.some)
            statements.insertMetadata(key, timestamp, head, origin) as head.segmentSize
          }

          def update(head: JournalHead) = {
            val update =
              if (head.seqNr >= deleteTo) {
                statements.updateDeleteTo(key, partitionOffset, timestamp, deleteTo)
              } else {
                statements.updateMetadata(key, partitionOffset, timestamp, deleteTo, deleteTo)
              }
            update as head.segmentSize
          }

          def delete(segmentSize: SegmentSize)(head: JournalHead) = {

            def delete(from: SeqNr, deleteTo: SeqNr) = {

              def segment(seqNr: SeqNr) = SegmentNr.unsafe(seqNr, segmentSize)

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
            segmentSize <- head.fold(insert)(update)
            _           <- head.foldMap[F[Unit]](delete(segmentSize))
          } yield {}
        }

        for {
          head   <- statements.selectMetadata(key)
          result <- delete(head).uncancelable
        } yield result
      }


      def save(topic: Topic, topicPointers: TopicPointers, timestamp: Instant) = {

        def insertOrUpdate(current: Option[Offset], partition: Partition, offset: Offset) = {
          current.fold {
            statements.insertPointer(
              topic = topic,
              partition = partition,
              offset = offset,
              created = timestamp,
              updated = timestamp)
          } { _ =>
            statements.updatePointer(
              topic = topic,
              partition = partition,
              offset = offset,
              timestamp = timestamp)
          }
        }

        def saveOne(partition: Partition, offset: Offset) = {
          for {
            current <- statements.selectPointer(topic, partition)
            result  <- insertOrUpdate(current, partition, offset)
          } yield result
        }

        def saveMany(pointers: Nel[(Partition, Offset)]) = {
          val partitions = pointers.map { case (partition, _) => partition }
          for {
            current <- statements.selectPointersIn(topic, partitions)
            result  <- pointers.parFoldMap { case (partition, offset) => insertOrUpdate(current.get(partition), partition, offset) }
          } yield result
        }

        Nel
          .fromList(topicPointers.values.toList)
          .foldMapM {
            case Nel((partition, offset), Nil) => saveOne(partition, offset)
            case pointers                      => saveMany(pointers)
          }
      }

      def pointers(topic: Topic) = {
        for {
          pointers <- statements.selectPointers(topic)
        } yield {
          TopicPointers(pointers)
        }
      }
    }
  }


  final case class Statements[F[_]](
    insertRecords   : JournalStatements.InsertRecords[F],
    deleteRecords   : JournalStatements.DeleteRecords[F],
    insertMetadata  : MetadataStatements.Insert[F],
    selectMetadata  : MetadataStatements.SelectHead[F],
    updateMetadata  : MetadataStatements.Update[F],
    updateSeqNr     : MetadataStatements.UpdateSeqNr[F],
    updateDeleteTo  : MetadataStatements.UpdateDeleteTo[F],
    selectPointer   : PointerStatements.Select[F],
    selectPointersIn: PointerStatements.SelectIn[F],
    selectPointers  : PointerStatements.SelectAll[F],
    insertPointer   : PointerStatements.Insert[F],
    updatePointer   : PointerStatements.Update[F],
    selectTopics    : PointerStatements.SelectTopics[F])

  object Statements {

    def apply[F[_]](implicit F: Statements[F]): Statements[F] = F

    def of[F[_] : Monad : Parallel : CassandraSession](schema: Schema): F[Statements[F]] = {
      val statements = (
        JournalStatements.InsertRecords.of[F](schema.journal),
        JournalStatements.DeleteRecords.of[F](schema.journal),
        MetadataStatements.Insert.of[F](schema.metadata),
        MetadataStatements.SelectHead.of[F](schema.metadata),
        MetadataStatements.Update.of[F](schema.metadata),
        MetadataStatements.UpdateSeqNr.of[F](schema.metadata),
        MetadataStatements.UpdateDeleteTo.of[F](schema.metadata),
        PointerStatements.Select.of[F](schema.pointer),
        PointerStatements.SelectIn.of[F](schema.pointer),
        PointerStatements.SelectAll.of[F](schema.pointer),
        PointerStatements.Insert.of[F](schema.pointer),
        PointerStatements.Update.of[F](schema.pointer),
        PointerStatements.SelectTopics.of[F](schema.pointer))
      statements.parMapN(Statements[F])
    }
  }
}