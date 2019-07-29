package com.evolutiongaming.kafka.journal.eventual.cassandra

import java.time.Instant

import cats.Parallel
import cats.implicits._
import com.evolutiongaming.kafka.journal._
import com.evolutiongaming.kafka.journal.eventual.EventualJournalSpec._
import com.evolutiongaming.kafka.journal.eventual.{EventualJournal, EventualJournalSpec, TopicPointers}
import com.evolutiongaming.kafka.journal.util.ConcurrentOf
import com.evolutiongaming.nel.Nel
import com.evolutiongaming.skafka.Topic
import com.evolutiongaming.sstream.Stream
import com.evolutiongaming.sstream.FoldWhile._

class EventualCassandraSpec extends EventualJournalSpec {
  import EventualCassandraSpec._

  "EventualCassandra" when {
    for {
      segmentSize <- Nel(2, 10, 1000)
      delete      <- List(true, false)
    } {
      s"segmentSize: $segmentSize, delete: $delete" should {
        test[StateT] { test =>
          val (_, result) = test(journals(segmentSize, delete)).run(State.Empty)
          result.pure[StateT]
        }
      }
    }
  }
}

object EventualCassandraSpec {

  val selectHead: HeadStatement.Select[StateT] = new HeadStatement.Select[StateT] {
    def apply(key: Key) = {
      StateT { state =>
        val head = state.heads.get(key)
        (state, head)
      }
    }
  }


  val selectPointers: PointerStatement.SelectPointers[StateT] = new PointerStatement.SelectPointers[StateT] {
    def apply(topic: Topic) = {
      StateT { state =>
        val pointer = state.pointers.getOrElse(topic, TopicPointers.Empty)
        (state, pointer)
      }
    }
  }


  implicit val parallel = Parallel.identity[StateT]


  implicit val eventualJournal: EventualJournal[StateT] = {

    val selectRecords = new JournalStatement.SelectRecords[StateT] {

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

    val statements = EventualCassandra.Statements(
      records = selectRecords,
      head = selectHead,
      pointers = selectPointers)

    EventualCassandra[StateT](statements)
  }


  def journals(segmentSize: Int, delete: Boolean): Journals[StateT] = {

    val replicatedJournal = {

      val insertRecords = new JournalStatement.InsertRecords[StateT] {

        def apply(key: Key, segment: SegmentNr, records: Nel[EventRecord]) = {
          StateT { state =>
            val journal = state.journal
            val events = journal.events(key, segment)
            val updated = events ++ records.toList.sortBy(_.event.seqNr)
            val state1 = state.copy(journal = journal.updated((key, segment), updated))
            (state1, ())
          }
        }
      }


      val deleteRecords = new JournalStatement.DeleteRecords[StateT] {

        def apply(key: Key, segment: SegmentNr, seqNr: SeqNr) = {
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


      val insertHead = new HeadStatement.Insert[StateT] {
        def apply(key: Key, timestamp: Instant, head: Head, origin: Option[Origin]) = {
          StateT { state =>
            val state1 = state.copy(heads = state.heads.updated(key, head))
            (state1, ())
          }
        }
      }


      val updateHead = new HeadStatement.Update[StateT] {
        def apply(key: Key, partitionOffset: PartitionOffset, timestamp: Instant, seqNr: SeqNr, deleteTo: SeqNr) = {
          StateT { state =>
            val heads = state.heads
            val state1 = for {
              head <- heads.get(key)
            } yield {
              val head1 = head.copy(partitionOffset = partitionOffset, seqNr = seqNr, deleteTo = Some(deleteTo))
              state.copy(heads = heads.updated(key, head1))
            }

            (state1 getOrElse state, ())
          }
        }
      }


      val updateSeqNr = new HeadStatement.UpdateSeqNr[StateT] {

        def apply(key: Key, partitionOffset: PartitionOffset, timestamp: Instant, seqNr: SeqNr) = {
          StateT { state =>
            val heads = state.heads
            val state1 = for {
              head <- heads.get(key)
            } yield {
              val head1 = head.copy(partitionOffset = partitionOffset, seqNr = seqNr)
              state.copy(heads = heads.updated(key, head1))
            }
            (state1 getOrElse state, ())
          }
        }
      }


      val updateDeleteTo = new HeadStatement.UpdateDeleteTo[StateT] {
        def apply(key: Key, partitionOffset: PartitionOffset, timestamp: Instant, deleteTo: SeqNr) = {
          StateT { state =>
            val heads = state.heads
            val state1 = for {
              head <- heads.get(key)
            } yield {
              val head1 = head.copy(partitionOffset = partitionOffset, deleteTo = Some(deleteTo))
              state.copy(heads = heads.updated(key, head1))
            }

            (state1 getOrElse state, ())
          }
        }
      }


      val insertPointer = new PointerStatement.Insert[StateT] {

        def apply(pointer: PointerInsert) = {
          StateT { state =>
            val pointers = state.pointers
            val topicPointers = pointers.getOrElse(pointer.topic, TopicPointers.Empty)
            val updated = topicPointers.copy(values = topicPointers.values.updated(pointer.partition, pointer.offset))
            val pointers1 = pointers.updated(pointer.topic, updated)
            (state.copy(pointers = pointers1), ())
          }
        }
      }

      val selectTopics = new PointerStatement.SelectTopics[StateT] {
        def apply() = {
          StateT { state =>
            (state, state.pointers.keys.toList)
          }
        }
      }

      implicit val statements = ReplicatedCassandra.Statements(
        insertRecords = insertRecords,
        deleteRecords = deleteRecords,
        insertHead = insertHead,
        selectHead = selectHead,
        updateHead = updateHead,
        updateSeqNr = updateSeqNr,
        updateDeleteTo = updateDeleteTo,
        insertPointer = insertPointer,
        selectPointers = selectPointers,
        selectTopics = selectTopics)

      implicit val ConcurrentId = ConcurrentOf.fromMonad[StateT]
      ReplicatedCassandra(segmentSize)
    }

    Journals(eventualJournal, replicatedJournal)
  }


  final case class State(
    journal: Map[(Key, SegmentNr), List[EventRecord]],
    heads: Map[Key, Head],
    pointers: Map[Topic, TopicPointers])

  object State {
    val Empty: State = State(journal = Map.empty, heads = Map.empty, pointers = Map.empty)
  }


  type StateT[A] = cats.data.StateT[cats.Id, State, A]

  object StateT {
    def apply[A](f: State => (State, A)): StateT[A] = cats.data.StateT[cats.Id, State, A](f)
  }


  implicit class JournalOps(val self: Map[(Key, SegmentNr), List[EventRecord]]) extends AnyVal {

    def events(key: Key, segment: SegmentNr): List[EventRecord] = {
      val composite = (key, segment)
      self.getOrElse(composite, Nil)
    }
  }
}
