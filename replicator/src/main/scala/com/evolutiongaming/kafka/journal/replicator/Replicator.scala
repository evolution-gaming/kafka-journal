package com.evolutiongaming.kafka.journal.replicator

import java.time.Instant
import java.util.UUID

import akka.actor.ActorSystem
import com.evolutiongaming.cassandra.{CassandraConfig, CreateCluster}
import com.evolutiongaming.kafka.journal.Alias.SeqNr
import com.evolutiongaming.kafka.journal.ConsumerHelper._
import com.evolutiongaming.kafka.journal.FutureHelper._
import com.evolutiongaming.kafka.journal.KafkaConverters._
import com.evolutiongaming.kafka.journal._
import com.evolutiongaming.kafka.journal.eventual._
import com.evolutiongaming.kafka.journal.eventual.cassandra.{EventualCassandraConfig, ReplicatedCassandra, SchemaConfig}
import com.evolutiongaming.safeakka.actor.ActorLog
import com.evolutiongaming.skafka._
import com.evolutiongaming.skafka.consumer._

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future, Promise}


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
      timestamp: Instant): Future[TopicPointers] = {

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

      val futures = for {
        (id, records) <- records.groupBy { case (record, _) => record.id }
      } yield {

        val (_, partitionOffset) = records.last

        def onNonEmpty(batch: ActionBatch.NonEmpty) = {
          val deleteTo = batch.deleteTo getOrElse SeqNr.Min
          val replicated = for {
            (record, partitionOffset) <- records
            action <- PartialFunction.condOpt(record.action) { case a: Action.Append => a }.toIterable
            if action.range.to > deleteTo // TODO special range operation ???
            event <- EventsSerializer.fromBytes(action.events).toList
            if event.seqNr > deleteTo
          } yield {
            ReplicatedEvent(event, action.timestamp, partitionOffset)
          }

          val updateTmp = UpdateTmp.DeleteToKnown(batch.deleteTo, replicated.toList)

          for {
            result <- journal.save(id, updateTmp, topic)
          } yield {
            val head = replicated.head
            val last = replicated.last
            val range = SeqRange(head.event.seqNr, last.event.seqNr)
            val deleteTo = batch.deleteTo
            log.info(s"replicated id: $id, range: $range, deleteTo: $deleteTo, partitionOffset: $partitionOffset")
            result
          }
        }

        def onDelete(batch: ActionBatch.DeleteTo) = {
          val deleteTo = batch.seqNr
          val updateTmp = UpdateTmp.DeleteUnbound(deleteTo)
          for {
            result <- journal.save(id, updateTmp, topic)
          } yield {
            log.info(s"replicated id: $id, deleteTo: $deleteTo, partitionOffset: $partitionOffset")
            result
          }
        }

        val headers = for {(record, _) <- records} yield record.action.header

        val batch = ActionBatch(headers)
        batch match {
          case batch: ActionBatch.NonEmpty => onNonEmpty(batch)
          case batch: ActionBatch.DeleteTo => onDelete(batch)
          case ActionBatch.Empty           => Future.unit
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
          if (diff.pointers.isEmpty) Future.unit
          else journal.savePointers(topic, diff)
        }

        for {_ <- result} yield pointers + diff
      }

      for {
        _ <- Future.sequence(futures)
        pointers <- savePointers()
      } yield pointers
    }

    // TODO cache state and not re-read it when kafka is broken
    def consume(topicPointers: TopicPointers) = {
      consumer.foldAsync(topicPointers, pollTimeout) { (topicPointers, records) =>
        shutdown match {
          case Some(shutdown) =>
            val future = consumer.close(closeTimeout)
            shutdown.completeWith(future)

            for {
              _ <- future.recover { case _ => () }
            } yield {
              (topicPointers, false)
            }

          case None =>

            if (records.values.nonEmpty) {
              for {
                state <- apply(topicPointers, records, Instant.now())
              } yield {
                (state, true)
              }
            } else {
              val result = (topicPointers, true)
              result.future
            }
        }
      }
    }

    val future = for {
      pointers <- journal.pointers(topic)
      _ <- consume(pointers)
    } yield {}

    future.failed.foreach { failure =>
      failure.printStackTrace()
    }

    () => {
      val promise = Promise[Unit]()
      shutdown = Some(promise)
      promise.future
    }
  }
}
