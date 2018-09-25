package com.evolutiongaming.kafka.journal.eventual.cassandra

import java.time.Instant

import com.evolutiongaming.concurrent.async.Async
import com.evolutiongaming.concurrent.async.AsyncConverters._
import com.evolutiongaming.kafka.journal.FoldWhileHelper.{Fold, _}
import com.evolutiongaming.kafka.journal.eventual.EventualJournalSpec._
import com.evolutiongaming.kafka.journal.eventual.{EventualJournalSpec, Pointer, TopicPointers}
import com.evolutiongaming.kafka.journal._
import com.evolutiongaming.nel.Nel
import com.evolutiongaming.skafka.Topic

class EventualCassandraSpec extends EventualJournalSpec {
  import EventualCassandraSpec._

  // TODO implement and test fetch threshold

  "EventualCassandra" when {
    for {
      segmentSize <- Nel(2, 10, 1000)
    } {
      s"segmentSize: $segmentSize" should {
        test(createJournals(segmentSize))
      }
    }
  }

  def createJournals(segmentSize: Int): () => Journals = () => {

    var journal = Map.empty[(Key, SegmentNr), List[ReplicatedEvent]]
    var metadataMap = Map.empty[Key, Metadata]
    var pointers = Map.empty[Topic, TopicPointers]

    val selectLastRecord = (key: Key, segment: SegmentNr, from: SeqNr) => {
      val records = journal.events(key, segment)
      val pointer = for {
        record <- records.lastOption
        seqNr = record.event.seqNr
        if seqNr >= from
      } yield {
        Pointer(
          seqNr = record.event.seqNr,
          partitionOffset = record.partitionOffset)
      }
      pointer.async
    }

    val selectMetadata = (key: Key) => metadataMap.get(key).async

    val selectPointers = (topic: Topic) => pointers.getOrElse(topic, TopicPointers.Empty).async

    val eventual = {

      val selectRecords = new JournalStatement.SelectRecords.Type {
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
        selectLastRecord = selectLastRecord,
        selectRecords = selectRecords,
        selectMetadata = selectMetadata,
        selectPointers = selectPointers)

      EventualCassandra(statements.async, Log.empty(Async.unit))
    }

    val replicated = {

      val insertRecords = (key: Key, segment: SegmentNr, replicated: Nel[ReplicatedEvent]) => {
        val events = journal.events(key, segment)
        val updated = events ++ replicated.toList.sortBy(_.event.seqNr)
        journal = journal.updated((key, segment), updated)
        Async.unit
      }

      val deleteRecords = (key: Key, segment: SegmentNr, seqNr: SeqNr) => {
        val events = journal.events(key, segment)
        val updated = events.dropWhile(_.event.seqNr <= seqNr)
        journal = journal.updated((key, segment), updated)
        Async.unit
      }

      val insertMetadata = (key: Key, metadata: Metadata, timestamp: Instant) => {
        metadataMap = metadataMap.updated(key, metadata)
        Async.unit
      }

      val updateMetadata = (key: Key, deleteTo: Option[SeqNr], timestamp: Instant) => {
        for {
          metadata <- metadataMap.get(key)
        } {
          metadataMap = metadataMap.updated(key, metadata.copy(deleteTo = deleteTo))
        }
        Async.unit
      }

      val insertPointer = (pointer: PointerInsert) => {
        val topicPointers = pointers.getOrElse(pointer.topic, TopicPointers.Empty)
        val updated = topicPointers.copy(values = topicPointers.values.updated(pointer.partition, pointer.offset))
        pointers = pointers.updated(pointer.topic, updated)
        Async.unit
      }

      val selectTopics = () => {
        pointers.keys.toList.async
      }

      val statements = ReplicatedCassandra.Statements(
        insertRecords = insertRecords,
        selectLastRecord = selectLastRecord,
        deleteRecords = deleteRecords,
        insertMetadata = insertMetadata,
        selectMetadata = selectMetadata,
        updateMetadata = updateMetadata,
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
