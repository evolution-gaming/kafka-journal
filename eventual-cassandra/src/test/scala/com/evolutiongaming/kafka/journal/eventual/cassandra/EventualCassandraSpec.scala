package com.evolutiongaming.kafka.journal.eventual.cassandra

import com.evolutiongaming.concurrent.async.Async
import com.evolutiongaming.concurrent.async.AsyncConverters._
import com.evolutiongaming.kafka.journal.AsyncImplicits._
import com.evolutiongaming.kafka.journal.FoldWhile._
import com.evolutiongaming.kafka.journal._
import com.evolutiongaming.kafka.journal.eventual.EventualJournalSpec._
import com.evolutiongaming.kafka.journal.eventual.{EventualJournalSpec, TopicPointers}
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

    val selectLastRecord: JournalStatement.SelectLastRecord.Type[Async] = (key, segment, from) => {
      val records = journal.events(key, segment)
      val pointer = for {
        record <- records.lastOption
        seqNr = record.event.seqNr
        if seqNr >= from
      } yield {
        record.pointer
      }
      pointer.async
    }

    val selectMetadata: MetadataStatement.Select.Type[Async] = key => {
      metadataMap.get(key).async
    }

    val selectPointers: PointerStatement.SelectPointers.Type[Async] = topic => {
      pointers.getOrElse(topic, TopicPointers.Empty).async
    }

    val eventual = {

      val selectRecords = new JournalStatement.SelectRecords.Type[Async] {
        def apply[S](key: Key, segment: SegmentNr, range: SeqRange, s: S)(f: Fold[S, ReplicatedEvent]) = {
          val events = journal.events(key, segment)
          val result = events.foldWhile(s) { (s, event) =>
            val seqNr = event.event.seqNr
            if (range contains seqNr) f(s, event)
            else s.switch(seqNr <= range.to)
          }
          result.async
        }
      }

      val statements = EventualCassandra.Statements(
        lastRecord = selectLastRecord, // TODO not used
        records = selectRecords,
        metadata = selectMetadata,
        selectPointers = selectPointers)

      EventualCassandra(statements.async, Log.empty(Async.unit))
    }

    val replicated = {

      val insertRecords: JournalStatement.InsertRecords.Type[Async] = (key, segment, replicated) => {
        val events = journal.events(key, segment)
        val updated = events ++ replicated.toList.sortBy(_.event.seqNr)
        journal = journal.updated((key, segment), updated)
        Async.unit
      }

      val deleteRecords: JournalStatement.DeleteRecords.Type[Async] = (key, segment, seqNr) => {
        if (delete) {
          val events = journal.events(key, segment)
          val updated = events.dropWhile(_.event.seqNr <= seqNr)
          journal = journal.updated((key, segment), updated)
        }
        Async.unit
      }

      val insertMetadata: MetadataStatement.Insert.Type[Async] = (key, timestamp, metadata, origin) => {
        metadataMap = metadataMap.updated(key, metadata)
        Async.unit
      }

      val updateMetadata: MetadataStatement.Update.Type[Async] = (key, partitionOffset, timestamp, seqNr, deleteTo) => {
        for {
          metadata <- metadataMap.get(key)
        } {
          val metadataNew = metadata.copy(partitionOffset = partitionOffset, seqNr = seqNr, deleteTo = Some(deleteTo))
          metadataMap = metadataMap.updated(key, metadataNew)
        }
        Async.unit
      }

      val updateSeqNr: MetadataStatement.UpdateSeqNr.Type[Async] = (key, partitionOffset, timestamp, seqNr) => {
        for {
          metadata <- metadataMap.get(key)
        } {
          val metadataNew = metadata.copy(partitionOffset = partitionOffset, seqNr = seqNr)
          metadataMap = metadataMap.updated(key, metadataNew)
        }
        Async.unit
      }

      val updateDeleteTo: MetadataStatement.UpdateDeleteTo.Type[Async] = (key, partitionOffset, timestamp, deleteTo) => {
        for {
          metadata <- metadataMap.get(key)
        } {
          val metadataNew = metadata.copy(partitionOffset = partitionOffset, deleteTo = Some(deleteTo))
          metadataMap = metadataMap.updated(key, metadataNew)
        }
        Async.unit
      }

      val insertPointer: PointerStatement.Insert.Type[Async] = pointer => {
        val topicPointers = pointers.getOrElse(pointer.topic, TopicPointers.Empty)
        val updated = topicPointers.copy(values = topicPointers.values.updated(pointer.partition, pointer.offset))
        pointers = pointers.updated(pointer.topic, updated)
        Async.unit
      }

      val selectTopics: PointerStatement.SelectTopics.Type[Async] = () => {
        pointers.keys.toList.async
      }

      val statements = ReplicatedCassandra.Statements(
        insertRecords = insertRecords,
        selectLastRecord = selectLastRecord,
        deleteRecords = deleteRecords,
        insertMetadata = insertMetadata,
        selectMetadata = selectMetadata,
        updateMetadata = updateMetadata,
        updateSeqNr = updateSeqNr,
        updateDeleteTo = updateDeleteTo,
        insertPointer = insertPointer,
        selectPointers = selectPointers,
        selectTopics = selectTopics)

      ReplicatedCassandra(statements.async, segmentSize)
    }
    Journals(eventual, replicated)
  }
}

object EventualCassandraSpec {

  implicit class JournalOps(val self: Map[(Key, SegmentNr), List[ReplicatedEvent]]) extends AnyVal {

    def events(key: Key, segment: SegmentNr): List[ReplicatedEvent] = {
      val composite = (key, segment)
      self.getOrElse(composite, Nil)
    }
  }
}
