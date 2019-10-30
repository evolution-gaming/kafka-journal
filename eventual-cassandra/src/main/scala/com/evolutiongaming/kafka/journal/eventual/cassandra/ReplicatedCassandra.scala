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
import com.evolutiongaming.scassandra.TableName
import com.evolutiongaming.skafka.{Offset, Partition, Topic}
import com.evolutiongaming.smetrics.MeasureDuration

import scala.annotation.tailrec
import scala.concurrent.duration.FiniteDuration


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
      val segmentOf = SegmentOf[F](Segments.default)
      val journal = apply[F](config.segmentSize, segmentOf, statements)
        .withLog(log)
      metrics
        .fold(journal) { metrics => journal.withMetrics(metrics) }
        .enhanceError
    }
  }

  def apply[F[_] : BracketThrowable : Parallel](
    segmentSize: SegmentSize,
    segmentOf: SegmentOf[F],
    statements: Statements[F]
  ): ReplicatedJournal[F] = {

    implicit val monoidUnit = Applicative.monoid[F, Unit]

    val metaJournal = statements.metaJournal

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
        expireAfter: Option[FiniteDuration],
        events: Nel[EventRecord]
      ) = {

        def append(segmentSize: SegmentSize) = {

          @tailrec
          def loop(
            events: List[EventRecord],
            s: Option[(Segment, Nel[EventRecord])],
            result: F[Unit]
          ): F[Unit] = {

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
                    case None       => loop(tail, (segment, head :: batch).some, result)
                    case Some(next) => loop(tail, (next, Nel.of(head)).some, insert(segment, batch))
                  }
                  case None                   => loop(tail, (Segment(seqNr, segmentSize), Nel.of(head)).some, result)
                }

              case Nil => s.fold(result) { case (segment, batch) => insert(segment, batch) }
            }
          }

          loop(events.toList, None, ().pure[F])
        }

        def appendAndSave(journalHead: Option[JournalHead], segment: SegmentNr) = {
          val seqNrLast = events.last.seqNr

          val (save, journalHead1) = journalHead.fold {
            val journalHead = JournalHead(
              partitionOffset = partitionOffset,
              segmentSize = segmentSize,
              seqNr = seqNrLast,
              deleteTo = events.head.seqNr.prev[Option])
            val origin = events.head.origin
            val insert = metaJournal.insert(key, segment, timestamp, journalHead, origin)
            (insert, journalHead)
          } { journalHead =>
            val update = metaJournal.updateSeqNr(key, segment, partitionOffset, timestamp, seqNrLast)
            (update, journalHead)
          }

          for {
            _ <- append(journalHead1.segmentSize)
            _ <- save
          } yield {}
        }

        for {
          segment     <- segmentOf(key)
          journalHead <- metaJournal.journalHead(key, segment)
          result      <- appendAndSave(journalHead, segment).uncancelable
        } yield result
      }


      def delete(
        key: Key,
        partitionOffset: PartitionOffset,
        timestamp: Instant,
        deleteTo: SeqNr,
        origin: Option[Origin]
      ) = {

        def delete(head: Option[JournalHead], segment: SegmentNr) = {

          def insert = {
            val head = JournalHead(
              partitionOffset = partitionOffset,
              segmentSize = segmentSize,
              seqNr = deleteTo,
              deleteTo = deleteTo.some)
            metaJournal.insert(key, segment, timestamp, head, origin) as head.segmentSize
          }

          def update(journalHead: JournalHead) = {
            val update =
              if (journalHead.seqNr >= deleteTo) {
                metaJournal.updateDeleteTo(key, segment, partitionOffset, timestamp, deleteTo)
              } else {
                metaJournal.update(key, segment, partitionOffset, timestamp, deleteTo, deleteTo)
              }
            update as journalHead.segmentSize
          }

          def delete(segmentSize: SegmentSize)(journalHead: JournalHead) = {

            def delete(from: SeqNr, deleteTo: SeqNr) = {

              def segment(seqNr: SeqNr) = SegmentNr(seqNr, segmentSize)

              (segment(from) to segment(deleteTo)).parFoldMap { segment =>
                statements.deleteRecords(key, segment, deleteTo)
              }
            }

            val deleteToFixed = journalHead.seqNr min deleteTo

            journalHead.deleteTo.fold {
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
          segment <- segmentOf(key)
          head    <- metaJournal.journalHead(key, segment)
          result  <- delete(head, segment).uncancelable
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


  trait MetaJournalStatements[F[_]] {

    def journalHead(key: Key, segment: SegmentNr): F[Option[JournalHead]]

    def insert(
      key: Key,
      segment: SegmentNr,
      timestamp: Instant,
      journalHead: JournalHead,
      origin: Option[Origin]
    ): F[Unit]

    def update(
      key: Key,
      segment: SegmentNr,
      partitionOffset: PartitionOffset,
      timestamp: Instant,
      seqNr: SeqNr,
      deleteTo: SeqNr
    ): F[Unit]

    def updateSeqNr(
      key: Key,
      segment: SegmentNr,
      partitionOffset: PartitionOffset,
      timestamp: Instant,
      seqNr: SeqNr
    ): F[Unit]

    def updateDeleteTo(
      key: Key,
      segment: SegmentNr,
      partitionOffset: PartitionOffset,
      timestamp: Instant,
      deleteTo: SeqNr
    ): F[Unit]
  }

  object MetaJournalStatements {

    def of[F[_] : Monad : Parallel : CassandraSession](schema: Schema): F[MetaJournalStatements[F]] = {
      val metadata = {
        val statements = (
          MetadataStatements.SelectJournalHead.of[F](schema.metadata),
          MetadataStatements.Insert.of[F](schema.metadata),
          MetadataStatements.Update.of[F](schema.metadata),
          MetadataStatements.UpdateSeqNr.of[F](schema.metadata),
          MetadataStatements.UpdateDeleteTo.of[F](schema.metadata))
        statements.parMapN(apply[F])
      }

      schema
        .metaJournal
        .fold {
          metadata
        } { metaJournal =>
          val select = MetadataStatements.Select.of[F](schema.metadata)
          val insert = cassandra.MetaJournalStatements.Insert.of[F](metaJournal)
          (of[F](metaJournal), metadata, select, insert).parMapN(apply[F])
        }
    }


    def of[F[_] : Monad : Parallel : CassandraSession](metaJournal: TableName): F[MetaJournalStatements[F]] = {
      val statements = (
        cassandra.MetaJournalStatements.SelectJournalHead.of[F](metaJournal),
        cassandra.MetaJournalStatements.Insert.of[F](metaJournal),
        cassandra.MetaJournalStatements.Update.of[F](metaJournal),
        cassandra.MetaJournalStatements.UpdateSeqNr.of[F](metaJournal),
        cassandra.MetaJournalStatements.UpdateDeleteTo.of[F](metaJournal))
      statements.parMapN(apply[F])
    }


    def apply[F[_] : Monad](
      metaJournal      : MetaJournalStatements[F],
      metadata         : MetaJournalStatements[F],
      selectMetadata   : MetadataStatements.Select[F],
      insertMetaJournal: cassandra.MetaJournalStatements.Insert[F]
    ): MetaJournalStatements[F] = {

      new MetaJournalStatements[F] {

        def journalHead(key: Key, segment: SegmentNr) = {
          metaJournal
            .journalHead(key, segment)
            .flatMap { journalHead =>
              journalHead.fold {
                selectMetadata(key).flatMap { entry =>
                  entry.traverse { entry =>
                    val journalHead = entry.journalHead
                    insertMetaJournal(
                      key = key,
                      segment = segment,
                      created = entry.created,
                      updated = entry.updated,
                      journalHead = journalHead,
                      origin = entry.origin
                    ) as journalHead
                  }
                }
              } { journalHead =>
                journalHead.some.pure[F]
              }
            }
        }

        def insert(
          key: Key,
          segment: SegmentNr,
          timestamp: Instant,
          journalHead: JournalHead,
          origin: Option[Origin]
        ) = {
          for {
            _ <- metadata.insert(key, segment, timestamp, journalHead, origin)
            _ <- metaJournal.insert(key, segment, timestamp, journalHead, origin)
          } yield {}
        }

        def update(
          key: Key,
          segment: SegmentNr,
          partitionOffset: PartitionOffset,
          timestamp: Instant,
          seqNr: SeqNr,
          deleteTo: SeqNr
        ) = {
          for {
            _ <- metaJournal.update(key, segment, partitionOffset, timestamp, seqNr, deleteTo)
            _ <- metadata.update(key, segment, partitionOffset, timestamp, seqNr, deleteTo)
          } yield {}
        }

        def updateSeqNr(
          key: Key,
          segment: SegmentNr,
          partitionOffset: PartitionOffset,
          timestamp: Instant,
          seqNr: SeqNr
        ) = {
          for {
            _ <- metaJournal.updateSeqNr(key, segment, partitionOffset, timestamp, seqNr)
            _ <- metadata.updateSeqNr(key, segment, partitionOffset, timestamp, seqNr)
          } yield {}
        }

        def updateDeleteTo(
          key: Key,
          segment: SegmentNr,
          partitionOffset: PartitionOffset,
          timestamp: Instant,
          deleteTo: SeqNr
        ) = {
          for {
            _ <- metaJournal.updateDeleteTo(key, segment, partitionOffset, timestamp, deleteTo)
            _ <- metadata.updateDeleteTo(key, segment, partitionOffset, timestamp, deleteTo)
          } yield {}
        }
      }
    }


    def apply[F[_]](
      selectJournalHead: cassandra.MetaJournalStatements.SelectJournalHead[F],
      insert           : cassandra.MetaJournalStatements.Insert[F],
      update           : cassandra.MetaJournalStatements.Update[F],
      updateSeqNr      : cassandra.MetaJournalStatements.UpdateSeqNr[F],
      updateDeleteTo   : cassandra.MetaJournalStatements.UpdateDeleteTo[F]
    ): MetaJournalStatements[F] = {

      val inset1 = insert
      val update1 = update
      val updateSeqNr1 = updateSeqNr
      val updateDeleteTo1 = updateDeleteTo

      new MetaJournalStatements[F] {

        def journalHead(key: Key, segment: SegmentNr) = selectJournalHead(key, segment)

        def insert(
          key: Key,
          segment: SegmentNr,
          timestamp: Instant,
          journalHead: JournalHead,
          origin: Option[Origin]
        ) = {
          inset1(key, segment, timestamp, timestamp, journalHead, origin)
        }

        def update(
          key: Key,
          segment: SegmentNr,
          partitionOffset: PartitionOffset,
          timestamp: Instant,
          seqNr: SeqNr,
          deleteTo: SeqNr
        ) = {
          update1(key, segment, partitionOffset, timestamp, seqNr, deleteTo)
        }

        def updateSeqNr(
          key: Key,
          segment: SegmentNr,
          partitionOffset: PartitionOffset,
          timestamp: Instant,
          seqNr: SeqNr
        ) = {
          updateSeqNr1(key, segment, partitionOffset, timestamp, seqNr)
        }

        def updateDeleteTo(
          key: Key,
          segment: SegmentNr,
          partitionOffset: PartitionOffset,
          timestamp: Instant,
          deleteTo: SeqNr
        ) = {
          updateDeleteTo1(key, segment, partitionOffset, timestamp, deleteTo)
        }
      }
    }


    def apply[F[_]](
      selectJournalHead: MetadataStatements.SelectJournalHead[F],
      insert           : MetadataStatements.Insert[F],
      update           : MetadataStatements.Update[F],
      updateSeqNr      : MetadataStatements.UpdateSeqNr[F],
      updateDeleteTo   : MetadataStatements.UpdateDeleteTo[F]
    ): MetaJournalStatements[F] = {

      val insert1 = insert
      val update1 = update
      val updateSeqNr1 = updateSeqNr
      val updateDeleteTo1 = updateDeleteTo

      new MetaJournalStatements[F] {

        def journalHead(key: Key, segment: SegmentNr) = selectJournalHead(key)

        def insert(
          key: Key,
          segment: SegmentNr,
          timestamp: Instant,
          journalHead: JournalHead,
          origin: Option[Origin]
        ) = {
          insert1(key, timestamp, journalHead, origin)
        }

        def update(
          key: Key,
          segment: SegmentNr,
          partitionOffset: PartitionOffset,
          timestamp: Instant,
          seqNr: SeqNr,
          deleteTo: SeqNr
        ) = {
          update1(key, partitionOffset, timestamp, seqNr, deleteTo)
        }

        def updateSeqNr(
          key: Key,
          segment: SegmentNr,
          partitionOffset: PartitionOffset,
          timestamp: Instant,
          seqNr: SeqNr
        ) = {
          updateSeqNr1(key, partitionOffset, timestamp, seqNr)
        }

        def updateDeleteTo(
          key: Key,
          segment: SegmentNr,
          partitionOffset: PartitionOffset,
          timestamp: Instant,
          deleteTo: SeqNr
        ) = {
          updateDeleteTo1(key, partitionOffset, timestamp, deleteTo)
        }
      }
    }
  }


  final case class Statements[F[_]](
    insertRecords   : JournalStatements.InsertRecords[F],
    deleteRecords   : JournalStatements.DeleteRecords[F],
    metaJournal     : MetaJournalStatements[F],
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
        MetaJournalStatements.of[F](schema),
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