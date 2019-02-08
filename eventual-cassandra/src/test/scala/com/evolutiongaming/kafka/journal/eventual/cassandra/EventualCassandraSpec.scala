package com.evolutiongaming.kafka.journal.eventual.cassandra

import cats.implicits._
import com.evolutiongaming.kafka.journal._
import com.evolutiongaming.kafka.journal.eventual.EventualJournalSpec._
import com.evolutiongaming.kafka.journal.eventual.{EventualJournalSpec, TopicPointers}
import com.evolutiongaming.kafka.journal.stream.FoldWhile._
import com.evolutiongaming.kafka.journal.stream.Stream
import com.evolutiongaming.kafka.journal.util.ConcurrentOf
import com.evolutiongaming.nel.Nel
import com.evolutiongaming.skafka.Topic

class EventualCassandraSpec extends EventualJournalSpec {
  import EventualCassandraSpec._

  // TODO implement and test fetch threshold

  "EventualCassandra" when {
    for {
      segmentSize <- Nel(2, 10, 1000)
      delete <- List(true, false)
    } {
      s"segmentSize: $segmentSize, delete: $delete" should {
        test(createJournals(segmentSize, delete))
      }
    }
  }

  def createJournals(segmentSize: Int, delete: Boolean): () => Journals = () => {

    // TODO use StateT
    var journal = Map.empty[(Key, SegmentNr), List[ReplicatedEvent]]
    var heads = Map.empty[Key, Head]
    var pointers = Map.empty[Topic, TopicPointers]

    val selectHead: HeadStatement.Select[cats.Id] = key => {
      heads.get(key)
    }

    val selectPointers: PointerStatement.SelectPointers[cats.Id] = topic => {
      pointers.getOrElse(topic, TopicPointers.Empty)
    }

    val eventual = {

      val selectRecords = new JournalStatement.SelectRecords[cats.Id] {

        def apply(key: Key, segment: SegmentNr, range: SeqRange) = {
          new Stream[cats.Id, ReplicatedEvent] {
            def foldWhileM[L, R](l: L)(f: (L, ReplicatedEvent) => cats.Id[Either[L, R]]) = {
              val events = journal.events(key, segment)
              events.foldWhileM[cats.Id, L, R](l) { (l, event) =>
                val seqNr = event.event.seqNr
                if (range contains seqNr) f(l, event)
                else l.asLeft[R].pure[cats.Id]
              }
            }
          }
        }
      }

      val statements = EventualCassandra.Statements(
        records = selectRecords,
        head = selectHead,
        pointers = selectPointers)

      EventualCassandra[cats.Id](statements)
    }

    val replicated = {

      val insertRecords: JournalStatement.InsertRecords[cats.Id] = (key, segment, replicated) => {
        val events = journal.events(key, segment)
        val updated = events ++ replicated.toList.sortBy(_.event.seqNr)
        journal = journal.updated((key, segment), updated)
      }

      val deleteRecords: JournalStatement.DeleteRecords[cats.Id] = (key, segment, seqNr) => {
        if (delete) {
          val events = journal.events(key, segment)
          val updated = events.dropWhile(_.event.seqNr <= seqNr)
          journal = journal.updated((key, segment), updated)
        }
      }

      val insertHead: HeadStatement.Insert[cats.Id] = (key, _, head, _) => {
        heads = heads.updated(key, head)
      }

      val updateHead: HeadStatement.Update[cats.Id] = (key, partitionOffset, _, seqNr, deleteTo) => {
        for {
          head <- heads.get(key)
        } {
          val metadataNew = head.copy(partitionOffset = partitionOffset, seqNr = seqNr, deleteTo = Some(deleteTo))
          heads = heads.updated(key, metadataNew)
        }
      }

      val updateSeqNr: HeadStatement.UpdateSeqNr[cats.Id] = (key, partitionOffset, _, seqNr) => {
        for {
          head <- heads.get(key)
        } {
          val head1 = head.copy(partitionOffset = partitionOffset, seqNr = seqNr)
          heads = heads.updated(key, head1)
        }
      }

      val updateDeleteTo: HeadStatement.UpdateDeleteTo[cats.Id] = (key, partitionOffset, _, deleteTo) => {
        for {
          head <- heads.get(key)
        } {
          val head1 = head.copy(partitionOffset = partitionOffset, deleteTo = Some(deleteTo))
          heads = heads.updated(key, head1)
        }
      }

      val insertPointer: PointerStatement.Insert[cats.Id] = pointer => {
        val topicPointers = pointers.getOrElse(pointer.topic, TopicPointers.Empty)
        val updated = topicPointers.copy(values = topicPointers.values.updated(pointer.partition, pointer.offset))
        pointers = pointers.updated(pointer.topic, updated)
      }

      val selectTopics: PointerStatement.SelectTopics[cats.Id] = () => {
        pointers.keys.toList
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

      implicit val ConcurrentId = ConcurrentOf.fromMonad[cats.Id]
      ReplicatedCassandra(segmentSize)
    }
    Journals(eventual, replicated)
  }
}

object EventualCassandraSpec {

  implicit val ParId: Par[cats.Id] = Par.sequential[cats.Id]

  implicit class JournalOps(val self: Map[(Key, SegmentNr), List[ReplicatedEvent]]) extends AnyVal {

    def events(key: Key, segment: SegmentNr): List[ReplicatedEvent] = {
      val composite = (key, segment)
      self.getOrElse(composite, Nil)
    }
  }
}
