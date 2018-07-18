package com.evolutiongaming.kafka.journal.replicator

import java.time.Instant
import java.util.UUID

import akka.actor.ActorSystem
import com.evolutiongaming.cassandra.{CassandraConfig, CreateCluster}
import com.evolutiongaming.concurrent.async.Async
import com.evolutiongaming.concurrent.async.AsyncConverters._
import com.evolutiongaming.kafka.journal.Alias.SeqNr
import com.evolutiongaming.kafka.journal.FoldWhileHelper._
import com.evolutiongaming.kafka.journal.KafkaConverters._
import com.evolutiongaming.kafka.journal._
import com.evolutiongaming.kafka.journal.eventual._
import com.evolutiongaming.kafka.journal.eventual.cassandra.{EventualCassandraConfig, ReplicatedCassandra, SchemaConfig}
import com.evolutiongaming.safeakka.actor.ActorLog
import com.evolutiongaming.skafka.consumer._
import com.evolutiongaming.skafka.{Bytes => _, _}

import scala.compat.Platform
import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.{Failure, Success}


// TODO refactor EventualDb api, make sure it does operate with Seq[Actions]
object Replicator {

  type Shutdown = () => Future[Unit]

  object Shutdown {
    val Empty: Shutdown = () => Future.unit
  }

  def apply(implicit system: ActorSystem, ec: ExecutionContext): Shutdown = {
    val groupId = UUID.randomUUID().toString
    val consumerConfig = ConsumerConfig.Default.copy(
      groupId = Some(groupId),
      autoOffsetReset = AutoOffsetReset.Earliest)
    val ecBlocking = ec // TODO
    val consumer = CreateConsumer[String, Bytes](consumerConfig, ecBlocking)
    val cassandraConfig = CassandraConfig.Default
    val cluster = CreateCluster(cassandraConfig)
    val session = cluster.connect()
    val schemaConfig = SchemaConfig.Default
    val config = EventualCassandraConfig.Default
    val eventualDb = ReplicatedCassandra(session, schemaConfig, config)
    apply(consumer, eventualDb)
  }

  def apply(
    consumer: Consumer[String, Bytes],
    journal: ReplicatedJournal,
    pollTimeout: FiniteDuration = 100.millis,
    closeTimeout: FiniteDuration = 10.seconds)(implicit
    ec: ExecutionContext, system: ActorSystem): Shutdown = {

    val topic = "journal"
    val topics = List(topic)
    consumer.subscribe(topics)
    // TODO seek to the beginning
    // TODO acknowledge ?

    val log = ActorLog(system, Replicator.getClass) prefixed topic

    // TODO replace with StateVar
    @volatile var shutdown = Option.empty[Promise[Unit]]


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
        (id, records) <- records.groupBy { case (record, _) => record.id }
      } yield {

        val (_, partitionOffset) = records.last

        def onNonEmpty(info: JournalInfo.NonEmpty) = {
          val deleteTo = info.deleteTo getOrElse SeqNr.Min
          val replicated = for {
            (record, partitionOffset) <- records
            action <- PartialFunction.condOpt(record.action) { case a: Action.Append => a }.toIterable
            if action.range.to > deleteTo // TODO special range operation ???
            event <- EventsSerializer.fromBytes(action.events).toList
            if event.seqNr > deleteTo
          } yield {
            ReplicatedEvent(event, action.timestamp, partitionOffset)
          }

          val updateTmp = UpdateTmp.DeleteToKnown(info.deleteTo, replicated.toList)
          val timestamp = Platform.currentTime
          for {
            result <- journal.save(id, updateTmp, topic)
          } yield {
            val head = replicated.head
            val last = replicated.last
            val range = SeqRange(head.event.seqNr, last.event.seqNr)
            val deleteTo = info.deleteTo
            val now = Platform.currentTime
            val duration = now - timestamp
            val latency = now - head.timestamp.toEpochMilli
            log.info(s"replicated in $duration|$latency ms, id: $id, range: $range, deleteTo: $deleteTo, partitionOffset: $partitionOffset")
            result
          }
        }

        def onDelete(info: JournalInfo.DeleteTo) = {
          val deleteTo = info.seqNr
          val updateTmp = UpdateTmp.DeleteUnbound(deleteTo)
          val timestamp = Platform.currentTime
          for {
            result <- journal.save(id, updateTmp, topic)
          } yield {
            val duration = Platform.currentTime - timestamp
            log.info(s"replicated in $duration ms, id: $id, deleteTo: $deleteTo, partitionOffset: $partitionOffset")
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

    // TODO cache state and not re-read it when kafka is broken
    def consume(pointers: TopicPointers) = {

      val fold = (pointers: TopicPointers) => {
        for {
          records <- consumer.poll(pollTimeout).async
          result <- shutdown match {
            case Some(shutdown) =>
              val future = consumer.close(closeTimeout)
              shutdown.completeWith(future)
              for {
                _ <- future.recover { case _ => () }.async
              } yield {
                pointers.stop
              }

            case None =>
              if (records.values.nonEmpty) {
                for {
                  pointers <- apply(pointers, records, Instant.now())
                } yield {
                  pointers.continue
                }
              } else {
                pointers.continue.async
              }
          }
        } yield result
      }

      fold.foldWhile(pointers)
    }

    val async = for {
      pointers <- journal.pointers(topic)
      _ <- consume(pointers)
    } yield {}

    async.onComplete {
      case Success(_)       =>
      case Failure(failure) => log.error(s"Replicator failed: $failure", failure)
    }

    () => {
      val promise = Promise[Unit]()
      shutdown = Some(promise)
      promise.future
    }
  }
}
