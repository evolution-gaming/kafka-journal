package com.evolutiongaming.kafka.journal.eventual.cassandra

import com.evolutiongaming.kafka.journal.FoldWhile._
import com.evolutiongaming.kafka.journal._
import com.evolutiongaming.kafka.journal.eventual.EventualJournalSpec._
import com.evolutiongaming.kafka.journal.eventual.{EventualJournalSpec, TopicPointers}
import com.evolutiongaming.kafka.journal.util.{ConcurrentOf, Par, ParOf}
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

    var journal = Map.empty[(Key, SegmentNr), List[ReplicatedEvent]]
    var metadataMap = Map.empty[Key, Metadata]
    var pointers = Map.empty[Topic, TopicPointers]

    val selectMetadata: MetadataStatement.Select[cats.Id] = key => {
      metadataMap.get(key)
    }

    val selectPointers: PointerStatement.SelectPointers[cats.Id] = topic => {
      pointers.getOrElse(topic, TopicPointers.Empty)
    }

    val eventual = {

      val selectRecords = new JournalStatement.SelectRecords[cats.Id] {
        def apply[S](key: Key, segment: SegmentNr, range: SeqRange, s: S)(f: Fold[S, ReplicatedEvent]) = {
          val events = journal.events(key, segment)
          events.foldWhile(s) { (s, event) =>
            val seqNr = event.event.seqNr
            if (range contains seqNr) f(s, event)
            else s.switch(seqNr <= range.to)
          }
        }
      }

      val statements = EventualCassandra.Statements(
        records = selectRecords,
        metadata = selectMetadata,
        pointers = selectPointers)

      EventualCassandra[cats.Id](statements, Log.empty)
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

      val insertMetadata: MetadataStatement.Insert[cats.Id] = (key, timestamp, metadata, origin) => {
        metadataMap = metadataMap.updated(key, metadata)
      }

      val updateMetadata: MetadataStatement.Update[cats.Id] = (key, partitionOffset, timestamp, seqNr, deleteTo) => {
        for {
          metadata <- metadataMap.get(key)
        } {
          val metadataNew = metadata.copy(partitionOffset = partitionOffset, seqNr = seqNr, deleteTo = Some(deleteTo))
          metadataMap = metadataMap.updated(key, metadataNew)
        }
      }

      val updateSeqNr: MetadataStatement.UpdateSeqNr[cats.Id] = (key, partitionOffset, timestamp, seqNr) => {
        for {
          metadata <- metadataMap.get(key)
        } {
          val metadataNew = metadata.copy(partitionOffset = partitionOffset, seqNr = seqNr)
          metadataMap = metadataMap.updated(key, metadataNew)
        }
      }

      val updateDeleteTo: MetadataStatement.UpdateDeleteTo[cats.Id] = (key, partitionOffset, timestamp, deleteTo) => {
        for {
          metadata <- metadataMap.get(key)
        } {
          val metadataNew = metadata.copy(partitionOffset = partitionOffset, deleteTo = Some(deleteTo))
          metadataMap = metadataMap.updated(key, metadataNew)
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
        insertMetadata = insertMetadata,
        selectMetadata = selectMetadata,
        updateMetadata = updateMetadata,
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

  implicit val ParId: Par[cats.Id] = ParOf.id

  implicit class JournalOps(val self: Map[(Key, SegmentNr), List[ReplicatedEvent]]) extends AnyVal {

    def events(key: Key, segment: SegmentNr): List[ReplicatedEvent] = {
      val composite = (key, segment)
      self.getOrElse(composite, Nil)
    }
  }
}
