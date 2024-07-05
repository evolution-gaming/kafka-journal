package com.evolutiongaming.kafka.journal.eventual.cassandra

import cats.Parallel
import cats.effect.IO
import cats.implicits.*
import com.evolutiongaming.catshelper.DataHelper.*
import com.evolutiongaming.kafka.journal.*
import com.evolutiongaming.kafka.journal.Journal.DataIntegrityConfig
import com.evolutiongaming.kafka.journal.eventual.*
import com.evolutiongaming.kafka.journal.eventual.EventualJournalSpec.*
import com.evolutiongaming.kafka.journal.util.Fail
import com.evolutiongaming.kafka.journal.util.TestTemporal.*
import com.evolutiongaming.skafka.{Offset, Topic}
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
      segments    <- List((Segments.min, Segments.old), (Segments.old, Segments.default))
      delete      <- List(true, false)
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

  val selectJournalHead: MetaJournalStatements.SelectJournalHead[StateT] = { (key, _) =>
    {
      StateT { state =>
        val metaJournal = state.metaJournal.get(key)
        (state, metaJournal)
      }
    }
  }

  val selectJournalPointer: MetaJournalStatements.SelectJournalPointer[StateT] = { (key, _) =>
    {
      StateT { state =>
        val pointer = state
          .metaJournal
          .get(key)
          .map { metadata => JournalPointer(metadata.partitionOffset, metadata.seqNr) }
        (state, pointer)
      }
    }
  }

  val selectOffset: PointerStatements.SelectOffset[StateT] = { (topic, partition) =>
    {
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

  val selectOffset2: Pointer2Statements.SelectOffset[StateT] = { (_, _) =>
    none[Offset].pure[StateT]
  }

  val selectPointer: PointerStatements.Select[StateT] = { (_, _) =>
    PointerStatements.Select.Result(Instant.EPOCH.some).some.pure[StateT]
  }

  val selectPointer2: Pointer2Statements.Select[StateT] = { (_, _) =>
    none[Pointer2Statements.Select.Result].pure[StateT]
  }

  val selectIds: MetaJournalStatements.SelectIds[StateT] = { (topic, segmentNr) =>
    {
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

  def eventualJournalOf(segmentNrsOf: SegmentNrsOf[StateT], segments: Segments): EventualJournal[StateT] = {

    val selectRecords = new JournalStatements.SelectRecords[StateT] {

      def apply(key: Key, segment: SegmentNr, range: SeqRange) = new Stream[StateT, EventRecord[EventualPayloadAndType]] {

        def foldWhileM[L, R](l: L)(f: (L, EventRecord[EventualPayloadAndType]) => StateT[Either[L, R]]) = {
          StateT { state =>
            val events = state.journal.events(key, segment)
            val result = events.foldWhileM[StateT, L, R](l) { (l, event) =>
              val seqNr = event.event.seqNr
              if (range contains seqNr) f(l, event)
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
        segmentNrsOf   = segmentNrsOf,
        journalHead    = selectJournalHead,
        journalPointer = selectJournalPointer,
        ids            = selectIds,
        segments       = segments,
      )

    val statements = EventualCassandra.Statements(selectRecords, metaJournalStatements, selectOffset, selectOffset2)

    EventualCassandra.apply1[StateT](statements, DataIntegrityConfig.Default)
  }

  def replicatedJournalOf(
    segmentSize: SegmentSize,
    delete: Boolean,
    segmentNrsOf: SegmentNrsOf[StateT],
  ): ReplicatedJournal[StateT] = {

    val insertRecords: JournalStatements.InsertRecords[StateT] = { (key, segment, records) =>
      {
        StateT { state =>
          val journal = state.journal
          val events  = journal.events(key, segment)
          val updated = events ++ records.toList.sortBy(_.event.seqNr)
          val state1  = state.copy(journal = journal.updated((key, segment), updated))
          (state1, ())
        }
      }
    }

    val deleteRecordsTo: JournalStatements.DeleteTo[StateT] = { (key, segment, seqNr) =>
      {
        StateT { state =>
          val state1 = {
            if (delete) {
              val journal = state.journal
              val events  = journal.events(key, segment)
              val updated = events.dropWhile(_.event.seqNr <= seqNr)
              state.copy(journal = journal.updated((key, segment), updated))
            } else {
              state
            }
          }
          (state1, ())
        }
      }
    }

    val deleteRecords: JournalStatements.Delete[StateT] = { (key, segment) =>
      {
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

    val insertMetaJournal: MetaJournalStatements.Insert[StateT] = { (key: Key, _, _, _, journalHead: JournalHead, _) =>
      {
        StateT { state =>
          val state1 = state.copy(metaJournal = state.metaJournal.updated(key, journalHead))
          (state1, ())
        }
      }
    }

    val updateMetaJournal: MetaJournalStatements.Update[StateT] = { (key, _, partitionOffset, _, seqNr, deleteTo) =>
      {
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

    val updateMetaJournalSeqNr: MetaJournalStatements.UpdateSeqNr[StateT] = { (key, _, partitionOffset, _, seqNr) =>
      {
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

    val updateMetaJournalExpiry: MetaJournalStatements.UpdateExpiry[StateT] = { (key, _, partitionOffset, _, seqNr, expiry) =>
      {
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

    val updateMetaJournalDeleteTo: MetaJournalStatements.UpdateDeleteTo[StateT] = { (key, _, partitionOffset, _, deleteTo) =>
      {
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

    val updateMetaJournalPartitionOffset: MetaJournalStatements.UpdatePartitionOffset[StateT] = { (key, _, partitionOffset, _) =>
      {
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

    val deleteMetaJournal: MetaJournalStatements.Delete[StateT] = { (key, _) =>
      {
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

    val deleteMetaJournalExpiry: MetaJournalStatements.DeleteExpiry[StateT] = { (key: Key, _: SegmentNr) =>
      {
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

    val insertPointer: PointerStatements.Insert[StateT] = { (topic, partition, offset, _, _) =>
      {
        StateT { state =>
          val pointers      = state.pointers
          val topicPointers = pointers.getOrElse(topic, TopicPointers.empty)
          val updated       = topicPointers.copy(values = topicPointers.values.updated(partition, offset))
          val pointers1     = pointers.updated(topic, updated)
          (state.copy(pointers = pointers1), ())
        }
      }
    }

    val insertPointer2: Pointer2Statements.Insert[StateT] = { (_, _, _, _, _) =>
      ().pure[StateT]
    }

    val updatePointer: PointerStatements.Update[StateT] = { (topic, partition, offset, _) =>
      {
        StateT { state =>
          val pointers      = state.pointers
          val topicPointers = pointers.getOrElse(topic, TopicPointers.empty)
          val updated       = topicPointers.copy(values = topicPointers.values.updated(partition, offset))
          val pointers1     = pointers.updated(topic, updated)
          (state.copy(pointers = pointers1), ())
        }
      }
    }

    val updatePointer2: Pointer2Statements.Update[StateT] = { (_, _, _, _) =>
      ().pure[StateT]
    }

    val updatePointerCreated2: Pointer2Statements.UpdateCreated[StateT] = { (_, _, _, _, _) =>
      ().pure[StateT]
    }

    val selectTopics: PointerStatements.SelectTopics[StateT] = { () =>
      {
        StateT { state =>
          (state, state.pointers.keySet.toSortedSet)
        }
      }
    }

    val selectTopics2: Pointer2Statements.SelectTopics[StateT] = { () =>
      SortedSet.empty[Topic].pure[StateT]
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
      selectOffset,
      selectOffset2,
      selectPointer,
      selectPointer2,
      insertPointer,
      insertPointer2,
      updatePointer,
      updatePointer2,
      updatePointerCreated2,
      selectTopics,
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
    val segmentNrsOf      = SegmentNrsOf[StateT](first = segmentsFirst, second = segmentsSecond)
    val replicatedJournal = replicatedJournalOf(segmentSize, delete, segmentNrsOf)
    val eventualJournal   = eventualJournalOf(segmentNrsOf, segmentsFirst max segmentsSecond)
    EventualAndReplicated(eventualJournal, replicatedJournal)
  }

  final case class State(
    journal: Map[(Key, SegmentNr), List[EventRecord[EventualPayloadAndType]]],
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

  implicit class JournalOps[A](val self: Map[(Key, SegmentNr), List[EventRecord[A]]]) extends AnyVal {

    def events(key: Key, segment: SegmentNr): List[EventRecord[A]] = {
      val composite = (key, segment)
      self.getOrElse(composite, Nil)
    }
  }
}
