package com.evolutiongaming.kafka.journal.replicator

import java.time.Instant
import java.util.concurrent.atomic.AtomicBoolean

import com.evolutiongaming.concurrent.async.Async
import com.evolutiongaming.concurrent.async.AsyncConverters._
import com.evolutiongaming.kafka.journal.Alias.SeqNr
import com.evolutiongaming.kafka.journal.FoldWhileHelper._
import com.evolutiongaming.kafka.journal.KafkaConverters._
import com.evolutiongaming.kafka.journal._
import com.evolutiongaming.kafka.journal.eventual._
import com.evolutiongaming.safeakka.actor.ActorLog
import com.evolutiongaming.skafka.consumer._
import com.evolutiongaming.skafka.{Bytes => _, _}

import scala.compat.Platform
import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import scala.language.existentials


trait TopicReplicator {
  def shutdown(): Async[Unit]
}

object TopicReplicator {

  def apply(
    topic: Topic,
    consumer: Consumer[String, Bytes],
    journal: ReplicatedJournal,
    log: ActorLog,
    pollTimeout: FiniteDuration = 100.millis)(implicit
    ec: ExecutionContext): TopicReplicator = {

    val topics = List(topic)
    consumer.subscribe(topics, None)
    // TODO seek to the beginning
    // TODO acknowledge ?


    // TODO handle that consumerRecords are not empty
    def apply(
      pointers: TopicPointers,
      consumerRecords: ConsumerRecords[String, Bytes],
      timestamp: Instant): Async[TopicPointers] = {

      // TODO avoid creating unnecessary collections
      val records = for {
        consumerRecords <- consumerRecords.values.values
        consumerRecord <- consumerRecords
        kafkaRecord <- consumerRecord.toKafkaRecord
        // TODO kafkaRecord.asInstanceOf[Action.User] ???
      } yield {
        val partitionOffset = PartitionOffset(
          partition = consumerRecord.partition,
          offset = consumerRecord.offset)
        (kafkaRecord, partitionOffset)
      }

      val asyncs = for {
        (key, records) <- records.groupBy { case (record, _) => record.key }
      } yield {

        val (last, partitionOffset) = records.last
        val time = Platform.currentTime
        val id = key.id

        def onNonEmpty(info: JournalInfo.NonEmpty) = {
          val deleteTo = info.deleteTo getOrElse SeqNr.Min
          val replicated = for {
            (record, partitionOffset) <- records
            action <- PartialFunction.condOpt(record.action) { case a: Action.Append => a }.toIterable
            if action.range.to > deleteTo
            event <- EventsSerializer.fromBytes(action.events).toList
            if event.seqNr > deleteTo
          } yield {
            ReplicatedEvent(event, action.timestamp, partitionOffset)
          }

          val replicate = Replicate.DeleteToKnown(info.deleteTo, replicated.toList)

          for {
            result <- journal.save(key, replicate, timestamp)
          } yield {
            val deleteTo = info.deleteTo
            val now = Platform.currentTime
            val saveDuration = now - time
            val latency = now - last.action.timestamp.toEpochMilli

            def range = replicated.headOption.fold("") { head =>
              val last = replicated.last
              val range = SeqRange(head.event.seqNr, last.event.seqNr)
              s" range: $range,"
            }

            log.info(s"replicated $id in ${ latency }ms,$range deleteTo: $deleteTo, partitionOffset: $partitionOffset, save: ${ saveDuration }ms")
            result
          }
        }

        def onDelete(info: JournalInfo.DeleteTo) = {
          val deleteTo = info.seqNr
          val replicate = Replicate.DeleteUnbound(deleteTo)
          val time = Platform.currentTime
          for {
            result <- journal.save(key, replicate, timestamp)
          } yield {
            val now = Platform.currentTime
            val saveDuration = now - time
            val latency = now - last.action.timestamp.toEpochMilli
            log.info(s"replicated $id in ${ latency }ms, deleteTo: $deleteTo, partitionOffset: $partitionOffset, save: ${ saveDuration }ms")
            result
          }
        }

        val headers = for {(record, _) <- records} yield record.action.header

        val info = JournalInfo(headers)
        info match {
          case info: JournalInfo.NonEmpty => onNonEmpty(info)
          case info: JournalInfo.DeleteTo => onDelete(info)
          case JournalInfo.Empty          => Async.unit
        }
      }

      def savePointers() = {
        val diff = {
          val pointers = for {
            (topicPartition, records) <- consumerRecords.values
            offset = records.foldLeft[Offset](0) { (offset, record) => record.offset max offset }
            if offset != 0
          } yield {
            (topicPartition.partition, offset)
          }
          TopicPointers(pointers)
        }

        val result = {
          if (diff.pointers.isEmpty) Async.unit
          else journal.savePointers(topic, diff)
        }

        for {_ <- result} yield pointers + diff
      }

      for {
        _ <- Async.foldUnit(asyncs)
        pointers <- savePointers()
      } yield pointers
    }

    val stop = new AtomicBoolean(false)

    // TODO cache state and not re-read it when kafka is broken
    def consume(pointers: TopicPointers) = {
      val fold = (pointers: TopicPointers) => {
        if (stop.get()) pointers.stop.async
        else {
          for {
            records <- consumer.poll(pollTimeout).async
            pointers <- {
              if (stop.get()) pointers.stop.async
              else {
                if (records.values.isEmpty) pointers.continue.async
                else apply(pointers, records, Instant.now()).map(_.continue)
              }
            }
          } yield pointers
        }
      }

      fold.foldWhile(pointers)
    }

    val async = for {
      pointers <- journal.pointers(topic)
      _ <- consume(pointers)
    } yield {}

    async.onFailure { failure => log.error(s"TopicReplicator failed: $failure", failure) }

    new TopicReplicator {

      def shutdown() = {
        stop.set(true)
        for {
          _ <- async
          _ <- consumer.close().async
        } yield {}
      }

      override def toString = s"TopicReplicator($topic)"
    }
  }
}


