package com.evolutiongaming.kafka.journal.eventual.cassandra

import java.time.Instant

import cats.Parallel
import cats.data.{IndexedStateT, NonEmptyList => Nel}
import cats.effect.ExitCase
import cats.implicits._
import com.evolutiongaming.catshelper.BracketThrowable
import com.evolutiongaming.kafka.journal._
import com.evolutiongaming.kafka.journal.eventual.EventualJournalSpec._
import com.evolutiongaming.kafka.journal.eventual.{EventualJournal, EventualJournalSpec, ReplicatedJournal, TopicPointers}
import com.evolutiongaming.kafka.journal.util.{BracketFromMonadError, ConcurrentOf}
import com.evolutiongaming.skafka.{Offset, Partition, Topic}
import com.evolutiongaming.sstream.FoldWhile._
import com.evolutiongaming.sstream.Stream

import scala.util.Try

// TODO test purge
class EventualCassandraSpec extends EventualJournalSpec {
  import EventualCassandraSpec._

  "EventualCassandra" when {
    for {
      segmentSize <- List(SegmentSize.min, SegmentSize.default, SegmentSize.max)
      segments    <- List(Segments.min, Segments.default)
      delete      <- List(true, false)
    } {
      s"segmentSize: $segmentSize, delete: $delete, segments: $segments" should {
        test[StateT] { test =>
          val journals = journalsOf(segmentSize, delete, segments)
          val (_, result) = test(journals)
            .run(State.empty)
            .get
          result.pure[StateT]
        }
      }
    }
  }
}

object EventualCassandraSpec {

  val selectJournalHead: MetadataStatements.SelectJournalHead[StateT] = {
    key: Key => {
      StateT { state =>
        val metadata = state.metadata.get(key)
        (state, metadata)
      }
    }
  }

  val selectJournalPointer: MetadataStatements.SelectJournalPointer[StateT] = {
    key: Key => {
      StateT { state =>
        val pointer = state
          .metadata
          .get(key)
          .map { metadata => JournalPointer(metadata.partitionOffset, metadata.seqNr) }
        (state, pointer)
      }
    }
  }


  val selectPointers: PointerStatements.SelectAll[StateT] = {
    topic: Topic => {
      StateT { state =>
        val pointer = state.pointers.getOrElse(topic, TopicPointers.empty)
        (state, pointer.values)
      }
    }
  }


  implicit val bracket: BracketThrowable[StateT] = new BracketFromMonadError[StateT, Throwable] {

    val F = IndexedStateT.catsDataMonadErrorForIndexedStateT(catsStdInstancesForTry)

    def bracketCase[A, B](
      acquire: StateT[A])(
      use: A => StateT[B])(
      release: (A, ExitCase[Throwable]) => StateT[Unit]
    ) = {

      def onError(a: A)(e: Throwable) = for {
        _ <- release(a, ExitCase.error(e))
        b <- raiseError[B](e)
      } yield b

      for {
        a <- acquire
        b <- handleErrorWith(use(a))(onError(a))
        _ <- release(a, ExitCase.complete)
      } yield b
    }
  }
  

  implicit val parallel: Parallel[StateT] = Parallel.identity[StateT]


  def eventualJournalOf(segmentOf: SegmentOf[StateT]): EventualJournal[StateT] = {

    val selectRecords = new JournalStatements.SelectRecords[StateT] {

      def apply(key: Key, segment: SegmentNr, range: SeqRange) = new Stream[StateT, EventRecord] {

        def foldWhileM[L, R](l: L)(f: (L, EventRecord) => StateT[Either[L, R]]) = {
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

    val metaJournalStatements = EventualCassandra.MetaJournalStatements(
      journalHead = selectJournalHead,
      journalPointer = selectJournalPointer)

    val statements = EventualCassandra.Statements(
      records = selectRecords,
      metaJournal = metaJournalStatements,
      pointers = selectPointers)

    EventualCassandra[StateT](statements, segmentOf)
  }


  def replicatedJournalOf(
    segmentSize: SegmentSize,
    delete: Boolean,
    segmentOf: SegmentOf[StateT]
  ): ReplicatedJournal[StateT] = {

    val insertRecords: JournalStatements.InsertRecords[StateT] = {
      (key: Key, segment: SegmentNr, records: Nel[EventRecord]) => {
        StateT { state =>
          val journal = state.journal
          val events = journal.events(key, segment)
          val updated = events ++ records.toList.sortBy(_.event.seqNr)
          val state1 = state.copy(journal = journal.updated((key, segment), updated))
          (state1, ())
        }
      }
    }


    val deleteRecords: JournalStatements.DeleteRecords[StateT] = {
      (key: Key, segment: SegmentNr, seqNr: SeqNr) => {
        StateT { state =>
          val state1 = {
            if (delete) {
              val journal = state.journal
              val events = journal.events(key, segment)
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


    val insertMetadata: MetadataStatements.Insert[StateT] = {
      (key: Key, _: Instant, head: JournalHead, _: Option[Origin]) => {
        StateT { state =>
          val state1 = state.copy(metadata = state.metadata.updated(key, head))
          (state1, ())
        }
      }
    }


    val updateMetadata: MetadataStatements.Update[StateT] = {
      (key: Key, partitionOffset: PartitionOffset, _: Instant, seqNr: SeqNr, deleteTo: SeqNr) => {
        StateT { state =>
          val metadata = state.metadata
          val state1 = for {
            entry <- metadata.get(key)
          } yield {
            val entry1 = entry.copy(partitionOffset = partitionOffset, seqNr = seqNr, deleteTo = Some(deleteTo))
            state.copy(metadata = metadata.updated(key, entry1))
          }

          (state1 getOrElse state, ())
        }
      }
    }


    val updateMetadataSeqNr: MetadataStatements.UpdateSeqNr[StateT] = {
      (key: Key, partitionOffset: PartitionOffset, _: Instant, seqNr: SeqNr) => {
        StateT { state =>
          val metadata = state.metadata
          val state1 = for {
            entry <- metadata.get(key)
          } yield {
            val entry1 = entry.copy(partitionOffset = partitionOffset, seqNr = seqNr)
            state.copy(metadata = metadata.updated(key, entry1))
          }
          (state1 getOrElse state, ())
        }
      }
    }


    val updateMetadataDeleteTo: MetadataStatements.UpdateDeleteTo[StateT] = {
      (key: Key, partitionOffset: PartitionOffset, _: Instant, deleteTo: SeqNr) => {
        StateT { state =>
          val metadata = state.metadata
          val state1 = for {
            entry <- metadata.get(key)
          } yield {
            val entry1 = entry.copy(partitionOffset = partitionOffset, deleteTo = Some(deleteTo))
            state.copy(metadata = metadata.updated(key, entry1))
          }

          (state1 getOrElse state, ())
        }
      }
    }


    val deleteMetadata: MetadataStatements.Delete[StateT] = {
      key: Key => {
        StateT { state =>
          val metadata = state.metadata
          val state1 = for {
            _ <- metadata.get(key)
          } yield {
            state.copy(metadata = metadata - key)
          }
          (state1 getOrElse state, ())
        }
      }
    }


    val insertPointer: PointerStatements.Insert[StateT] = {
      (topic: Topic, partition: Partition, offset: Offset, _: Instant, _: Instant) => {
        StateT { state =>
          val pointers = state.pointers
          val topicPointers = pointers.getOrElse(topic, TopicPointers.empty)
          val updated = topicPointers.copy(values = topicPointers.values.updated(partition, offset))
          val pointers1 = pointers.updated(topic, updated)
          (state.copy(pointers = pointers1), ())
        }
      }
    }


    val updatePointer: PointerStatements.Update[StateT] = {
      (topic: Topic, partition: Partition, offset: Offset, _: Instant) => {
        StateT { state =>
          val pointers = state.pointers
          val topicPointers = pointers.getOrElse(topic, TopicPointers.empty)
          val updated = topicPointers.copy(values = topicPointers.values.updated(partition, offset))
          val pointers1 = pointers.updated(topic, updated)
          (state.copy(pointers = pointers1), ())
        }
      }
    }


    val selectPointer: PointerStatements.Select[StateT] = {
      (topic: Topic, partition: Partition) => {
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


    val selectPointersIn: PointerStatements.SelectIn[StateT] = {
      (topic: Topic, partitions: Nel[Partition]) => {
        StateT { state =>
          val pointers = state
            .pointers
            .getOrElse(topic, TopicPointers.empty)
            .values
          val result = for {
            partition <- partitions.toList
            offset    <- pointers.get(partition)
          } yield {
            (partition, offset)
          }

          (state, result.toMap)
        }
      }
    }


    val selectTopics: PointerStatements.SelectTopics[StateT] = {
      () => {
        StateT { state =>
          (state, state.pointers.keys.toList)
        }
      }
    }

    val metadata = ReplicatedCassandra.MetaJournalStatements(
      selectJournalHead,
      insertMetadata,
      updateMetadata,
      updateMetadataSeqNr,
      updateMetadataDeleteTo,
      deleteMetadata)

    val statements = ReplicatedCassandra.Statements(
      insertRecords = insertRecords,
      deleteRecords = deleteRecords,
      metaJournal = metadata,
      selectPointer = selectPointer,
      selectPointersIn = selectPointersIn,
      selectPointers = selectPointers,
      insertPointer = insertPointer,
      updatePointer = updatePointer,
      selectTopics = selectTopics)

    implicit val concurrentId = ConcurrentOf.fromMonad[StateT]
    ReplicatedCassandra(segmentSize, segmentOf, statements)
  }

  def journalsOf(
    segmentSize: SegmentSize,
    delete: Boolean,
    segments: Segments
  ): Journals[StateT] = {

    val segmentOf = SegmentOf[StateT](segments)

    val replicatedJournal = replicatedJournalOf(segmentSize, delete, segmentOf)

    val eventualJournal = eventualJournalOf(segmentOf)
    Journals(eventualJournal, replicatedJournal)
  }


  final case class State(
    journal: Map[(Key, SegmentNr), List[EventRecord]],
    metadata: Map[Key, JournalHead],
    pointers: Map[Topic, TopicPointers])

  object State {
    val empty: State = State(journal = Map.empty, metadata = Map.empty, pointers = Map.empty)
  }


  type StateT[A] = cats.data.StateT[Try, State, A]

  object StateT {
    def apply[A](f: State => (State, A)): StateT[A] = cats.data.StateT[Try, State, A] { a => f(a).pure[Try]}
  }


  implicit class JournalOps(val self: Map[(Key, SegmentNr), List[EventRecord]]) extends AnyVal {

    def events(key: Key, segment: SegmentNr): List[EventRecord] = {
      val composite = (key, segment)
      self.getOrElse(composite, Nil)
    }
  }
}
