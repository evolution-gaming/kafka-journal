package com.evolution.kafka.journal.eventual.cassandra

import cats.Parallel
import cats.data.NonEmptyList as Nel
import cats.effect.IO
import cats.syntax.all.*
import com.evolution.kafka.journal.*
import com.evolution.kafka.journal.Journal.DataIntegrityConfig
import com.evolution.kafka.journal.eventual.*
import com.evolution.kafka.journal.eventual.EventualJournalSpec.*
import com.evolution.kafka.journal.eventual.cassandra.JournalStatements.JournalRecord
import com.evolution.kafka.journal.util.Fail
import com.evolution.kafka.journal.util.TestTemporal.*
import com.evolutiongaming.catshelper.DataHelper.IterableOps1DataHelper
import com.evolutiongaming.catshelper.Log
import com.evolutiongaming.skafka.{Offset, Partition, Topic}
import com.evolutiongaming.sstream.FoldWhile.*
import com.evolutiongaming.sstream.Stream

import java.time.{Instant, ZoneOffset}
import scala.collection.immutable.SortedSet

// TODO expiry: test purge
class EventualCassandraSpec extends EventualJournalSpec {
  import EventualCassandraSpec.*
  import cats.effect.unsafe.implicits.global

  "EventualCassandra" when {
    for {
      segmentSize <- List(SegmentSize.min, SegmentSize.default, SegmentSize.max)
      segments <- List((Segments.min, Segments.old), (Segments.old, Segments.default))
      delete <- List(true, false)
    } {
      s"segmentSize: $segmentSize, delete: $delete, segments: $segments" should {
        test[StateT] { test =>
          val (segmentsFirst, segmentsSecond) = segments
          val journals = journalsOf(segmentSize, delete, segmentsFirst = segmentsFirst, segmentsSecond = segmentsSecond)
          val (_, result) = test(journals)
            .run(State.empty)
            .unsafeRunSync()
          result.pure[StateT]
        }
      }
    }
  }
}

object EventualCassandraSpec {

  val selectJournalHead: MetaJournalStatements.SelectJournalHead[StateT] =
    new MetaJournalStatements.SelectJournalHead[StateT] {

      def apply(key: Key, segment: SegmentNr): StateT[Option[JournalHead]] = {
        StateT { state =>
          val metaJournal = state.metaJournal.get(key)
          (state, metaJournal)
        }
      }
    }

  val selectJournalPointer: MetaJournalStatements.SelectJournalPointer[StateT] =
    new MetaJournalStatements.SelectJournalPointer[StateT] {

      def apply(key: Key, segment: SegmentNr): StateT[Option[JournalPointer]] = {
        StateT { state =>
          val pointer = state
            .metaJournal
            .get(key)
            .map { metadata => JournalPointer(metadata.partitionOffset, metadata.seqNr) }
          (state, pointer)
        }
      }
    }

  val selectOffset2: Pointer2Statements.SelectOffset[StateT] = new Pointer2Statements.SelectOffset[StateT] {

    def apply(topic: Topic, partition: Partition): StateT[Option[Offset]] = {
      StateT { state =>
        val offset = state
          .pointers
          .getOrElse(topic, TopicPointers.empty)
          .values
          .get(partition)
        (state, offset)
      }
    }
  }

  val selectPointer2: Pointer2Statements.Select[StateT] = new Pointer2Statements.Select[StateT] {

    def apply(topic: Topic, partition: Partition): StateT[Option[Pointer2Statements.Select.Result]] =
      Pointer2Statements.Select.Result(Instant.EPOCH.some).some.pure[StateT]
  }

  val selectIds: MetaJournalStatements.SelectIds[StateT] = new MetaJournalStatements.SelectIds[StateT] {

    def apply(topic: Topic, segmentNr: SegmentNr): Stream[StateT, String] = {
      if (segmentNr === SegmentNr.min) {
        Stream.lift {
          StateT { state =>
            val ids = state
              .metaJournal
              .keys
              .toList
              .collect { case key if key.topic === topic => key.id }
            (state, Stream[StateT].apply(ids))
          }
        }.flatten
      } else {
        Stream.empty[StateT, String]
      }
    }
  }

  implicit val failStateT: Fail[StateT] = Fail.lift[StateT]

  implicit val parallelStateT: Parallel[StateT] = Parallel.identity[StateT]

  implicit val log: Log[StateT] = Log.empty[StateT]

  def eventualJournalOf(segmentNrsOf: SegmentNrs.Of[StateT], segments: Segments): EventualJournal[StateT] = {

    val selectRecords = new JournalStatements.SelectRecords[StateT] {

      def apply(key: Key, segment: SegmentNr, range: SeqRange): Stream[StateT, JournalRecord] =
        new Stream[StateT, JournalRecord] {

          def foldWhileM[L, R](l: L)(f: (L, JournalRecord) => StateT[Either[L, R]]): StateT[Either[L, R]] = {
            StateT { state =>
              val records = state.journal.records(key, segment)
              val result = records.foldWhileM[StateT, L, R](l) { (l, record) =>
                val seqNr = record.event.event.seqNr
                if (range contains seqNr) f(l, record)
                else l.asLeft[R].pure[StateT]
              }
              (state, result)
            }.flatten
          }
        }
    }

    val metaJournalStatements = EventualCassandra
      .MetaJournalStatements
      .fromMetaJournal(
        segmentNrsOf = segmentNrsOf,
        journalHead = selectJournalHead,
        journalPointer = selectJournalPointer,
        ids = selectIds,
        segments = segments,
      )

    val statements = EventualCassandra.Statements(selectRecords, metaJournalStatements, selectOffset2)

    EventualCassandra.apply[StateT](statements, DataIntegrityConfig.Default)
  }

  def replicatedJournalOf(
    segmentSize: SegmentSize,
    delete: Boolean,
    segmentNrsOf: SegmentNrs.Of[StateT],
  ): ReplicatedJournal[StateT] = {

    val insertRecords: JournalStatements.InsertRecords[StateT] = new JournalStatements.InsertRecords[StateT] {

      def apply(key: Key, segment: SegmentNr, insert: Nel[JournalRecord]): StateT[Unit] = {
        StateT { state =>
          val journal = state.journal
          val persisted = journal.records(key, segment)
          val updated = persisted ++ insert.toList.sortBy(_.event.event.seqNr)
          val state1 = state.copy(journal = journal.updated((key, segment), updated))
          (state1, ())
        }
      }
    }

    val deleteRecordsTo: JournalStatements.DeleteTo[StateT] = new JournalStatements.DeleteTo[StateT] {

      def apply(key: Key, segment: SegmentNr, seqNr: SeqNr): StateT[Unit] = {
        StateT { state =>
          val state1 = {
            if (delete) {
              val journal = state.journal
              val records = journal.records(key, segment)
              val updated = records.dropWhile(_.event.event.seqNr <= seqNr)
              state.copy(journal = journal.updated((key, segment), updated))
            } else {
              state
            }
          }
          (state1, ())
        }
      }
    }

    val deleteRecords: JournalStatements.Delete[StateT] = new JournalStatements.Delete[StateT] {

      def apply(key: Key, segment: SegmentNr): StateT[Unit] = {
        StateT { state =>
          val state1 = {
            if (delete) {
              state.copy(journal = state.journal.updated((key, segment), List.empty))
            } else {
              state
            }
          }
          (state1, ())
        }
      }
    }

    val insertMetaJournal: MetaJournalStatements.Insert[StateT] = new MetaJournalStatements.Insert[StateT] {

      def apply(
        key: Key,
        segment: SegmentNr,
        created: Instant,
        updated: Instant,
        journalHead: JournalHead,
        origin: Option[Origin],
      ): StateT[Unit] = {
        StateT { state =>
          val state1 = state.copy(metaJournal = state.metaJournal.updated(key, journalHead))
          (state1, ())
        }
      }
    }

    val updateMetaJournal: MetaJournalStatements.Update[StateT] = new MetaJournalStatements.Update[StateT] {

      def apply(
        key: Key,
        segment: SegmentNr,
        partitionOffset: PartitionOffset,
        timestamp: Instant,
        seqNr: SeqNr,
        deleteTo: DeleteTo,
      ): StateT[Unit] = {
        StateT { state =>
          val metaJournal = state.metaJournal
          val state1 = for {
            entry <- metaJournal.get(key)
          } yield {
            val entry1 = entry.copy(partitionOffset = partitionOffset, seqNr = seqNr, deleteTo = deleteTo.some)
            state.copy(metaJournal = metaJournal.updated(key, entry1))
          }

          (state1 getOrElse state, ())
        }
      }
    }

    val updateMetaJournalSeqNr: MetaJournalStatements.UpdateSeqNr[StateT] =
      new MetaJournalStatements.UpdateSeqNr[StateT] {

        def apply(
          key: Key,
          segment: SegmentNr,
          partitionOffset: PartitionOffset,
          timestamp: Instant,
          seqNr: SeqNr,
        ): StateT[Unit] = {
          StateT { state =>
            val metaJournal = state.metaJournal
            val state1 = for {
              entry <- metaJournal.get(key)
            } yield {
              val entry1 = entry.copy(partitionOffset = partitionOffset, seqNr = seqNr)
              state.copy(metaJournal = metaJournal.updated(key, entry1))
            }
            (state1 getOrElse state, ())
          }
        }
      }

    val updateMetaJournalExpiry: MetaJournalStatements.UpdateExpiry[StateT] =
      new MetaJournalStatements.UpdateExpiry[StateT] {

        def apply(
          key: Key,
          segment: SegmentNr,
          partitionOffset: PartitionOffset,
          timestamp: Instant,
          seqNr: SeqNr,
          expiry: Expiry,
        ): StateT[Unit] = {
          StateT { state =>
            val metaJournal = state.metaJournal
            val state1 = for {
              entry <- metaJournal.get(key)
            } yield {
              val entry1 = entry.copy(partitionOffset = partitionOffset, seqNr = seqNr, expiry = expiry.some)
              state.copy(metaJournal = metaJournal.updated(key, entry1))
            }
            (state1 getOrElse state, ())
          }
        }
      }

    val updateMetaJournalDeleteTo: MetaJournalStatements.UpdateDeleteTo[StateT] =
      new MetaJournalStatements.UpdateDeleteTo[StateT] {

        def apply(
          key: Key,
          segment: SegmentNr,
          partitionOffset: PartitionOffset,
          timestamp: Instant,
          deleteTo: DeleteTo,
        ): StateT[Unit] = {
          StateT { state =>
            val metaJournal = state.metaJournal
            val state1 = for {
              entry <- metaJournal.get(key)
            } yield {
              val entry1 = entry.copy(partitionOffset = partitionOffset, deleteTo = deleteTo.some)
              state.copy(metaJournal = metaJournal.updated(key, entry1))
            }

            (state1 getOrElse state, ())
          }
        }
      }

    val updateMetaJournalPartitionOffset: MetaJournalStatements.UpdatePartitionOffset[StateT] =
      new MetaJournalStatements.UpdatePartitionOffset[StateT] {

        def apply(
          key: Key,
          segment: SegmentNr,
          partitionOffset: PartitionOffset,
          timestamp: Instant,
        ): StateT[Unit] = {
          StateT { state =>
            val metaJournal = state.metaJournal
            val state1 = metaJournal
              .get(key)
              .map { entry =>
                val entry1 = entry.copy(partitionOffset = partitionOffset)
                state.copy(metaJournal = metaJournal.updated(key, entry1))
              }
              .getOrElse { state }
            (state1, ())
          }
        }
      }

    val deleteMetaJournal: MetaJournalStatements.Delete[StateT] = new MetaJournalStatements.Delete[StateT] {

      def apply(key: Key, segment: SegmentNr): StateT[Unit] = {
        StateT { state =>
          val metaJournal = state.metaJournal
          val state1 = for {
            _ <- metaJournal.get(key)
          } yield {
            state.copy(metaJournal = metaJournal - key)
          }
          (state1 getOrElse state, ())
        }
      }
    }

    val deleteMetaJournalExpiry: MetaJournalStatements.DeleteExpiry[StateT] =
      new MetaJournalStatements.DeleteExpiry[StateT] {

        def apply(key: Key, segment: SegmentNr): StateT[Unit] = {
          StateT { state =>
            val metaJournal = state.metaJournal
            val state1 = for {
              entry <- metaJournal.get(key)
            } yield {
              val entry1 = entry.copy(expiry = none)
              state.copy(metaJournal = metaJournal.updated(key, entry1))
            }

            (state1 getOrElse state, ())
          }
        }
      }

    val insertPointer2: Pointer2Statements.Insert[StateT] = new Pointer2Statements.Insert[StateT] {

      def apply(
        topic: Topic,
        partition: Partition,
        offset: Offset,
        created: Instant,
        updated: Instant,
      ): StateT[Unit] = {
        StateT { state =>
          val pointers = state.pointers
          val topicPointers = pointers.getOrElse(topic, TopicPointers.empty)
          val updated = topicPointers.copy(values = topicPointers.values.updated(partition, offset))
          val pointers1 = pointers.updated(topic, updated)
          (state.copy(pointers = pointers1), ())
        }
      }
    }

    val updatePointer2: Pointer2Statements.Update[StateT] = new Pointer2Statements.Update[StateT] {

      def apply(
        topic: Topic,
        partition: Partition,
        offset: Offset,
        timestamp: Instant,
      ): StateT[Unit] = {
        StateT { state =>
          val pointers = state.pointers
          val topicPointers = pointers.getOrElse(topic, TopicPointers.empty)
          val updated = topicPointers.copy(values = topicPointers.values.updated(partition, offset))
          val pointers1 = pointers.updated(topic, updated)
          (state.copy(pointers = pointers1), ())
        }
      }
    }

    val selectTopics2: Pointer2Statements.SelectTopics[StateT] = new Pointer2Statements.SelectTopics[StateT] {

      def apply(): StateT[SortedSet[Topic]] = {
        StateT { state =>
          (state, state.pointers.keySet.toSortedSet)
        }
      }
    }

    val metaJournal = ReplicatedCassandra.MetaJournalStatements(
      selectJournalHead,
      insertMetaJournal,
      updateMetaJournal,
      updateMetaJournalSeqNr,
      updateMetaJournalExpiry,
      updateMetaJournalDeleteTo,
      updateMetaJournalPartitionOffset,
      deleteMetaJournal,
      deleteMetaJournalExpiry,
    )

    val statements = ReplicatedCassandra.Statements(
      insertRecords,
      deleteRecordsTo,
      deleteRecords,
      metaJournal,
      selectOffset2,
      selectPointer2,
      insertPointer2,
      updatePointer2,
      selectTopics2,
    )

    ReplicatedCassandra(segmentSize, segmentNrsOf, statements, ExpiryService(ZoneOffset.UTC))
  }

  def journalsOf(
    segmentSize: SegmentSize,
    delete: Boolean,
    segmentsFirst: Segments,
    segmentsSecond: Segments,
  ): EventualAndReplicated[StateT] = {
    val segmentNrsOf = SegmentNrs.Of[StateT](first = segmentsFirst, second = segmentsSecond)
    val replicatedJournal = replicatedJournalOf(segmentSize, delete, segmentNrsOf)
    val eventualJournal = eventualJournalOf(segmentNrsOf, segmentsFirst max segmentsSecond)
    EventualAndReplicated(eventualJournal, replicatedJournal)
  }

  final case class State(
    journal: Map[(Key, SegmentNr), List[JournalRecord]],
    metaJournal: Map[Key, JournalHead],
    pointers: Map[Topic, TopicPointers],
  )

  object State {
    val empty: State = State(journal = Map.empty, metaJournal = Map.empty, pointers = Map.empty)
  }

  type StateT[A] = cats.data.StateT[IO, State, A]

  object StateT {
    def apply[A](f: State => (State, A)): StateT[A] = cats.data.StateT[IO, State, A] { a => IO.delay(f(a)) }
  }

  implicit class JournalOps(val self: Map[(Key, SegmentNr), List[JournalRecord]]) extends AnyVal {

    def records(key: Key, segment: SegmentNr): List[JournalRecord] = {
      self.getOrElse((key, segment), Nil)
    }
  }
}
