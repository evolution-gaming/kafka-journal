package akka.persistence.kafka.journal.replicator

import java.time.Instant
import java.util.UUID

import akka.actor.ActorSystem
import com.evolutiongaming.cassandra.{CassandraConfig, CreateCluster}
import com.evolutiongaming.kafka.journal.Alias.SeqNr
import com.evolutiongaming.kafka.journal.ConsumerHelper._
import com.evolutiongaming.kafka.journal.KafkaConverters._
import com.evolutiongaming.kafka.journal._
import com.evolutiongaming.kafka.journal.eventual._
import com.evolutiongaming.kafka.journal.eventual.cassandra.{EventualCassandraConfig, EventualDbCassandra, SchemaConfig}
import com.evolutiongaming.safeakka.actor.ActorLog
import com.evolutiongaming.skafka.consumer._
import com.evolutiongaming.skafka.{TopicPartition, _}

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
    val eventualDb = EventualDbCassandra(session, schemaConfig, config)
    apply(consumer, eventualDb)
  }

  def apply(
    consumer: Consumer[String, Bytes],
    db: EventualDb,
    pollTimeout: FiniteDuration = 100.millis,
    closeTimeout: FiniteDuration = 10.seconds)(implicit
    ec: ExecutionContext, system: ActorSystem): Shutdown = {

    //    val executorService = Executors.newSingleThreadExecutor()
    //    val executionContext = ExecutionContext.fromExecutor(executorService)

    val topic = "journal"
    val topics = List(topic)
    consumer.subscribe(topics)

    val log = ActorLog(system, Replicator.getClass) prefixed topic

    // TODO replace with StateVar
    @volatile var shutdown = Option.empty[Promise[Unit]]


    case class State(pointers: Map[TopicPartition, Offset])


    // TODO handle that consumerRecords are not empty
    def apply(state: State, consumerRecords: ConsumerRecords[String, Bytes]): Future[State] = {

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

        val actions = for {(record, _) <- records} yield record.action

        def onNonEmpty(batch: ActionBatch.NonEmpty) = {
          val deleteTo = batch.deleteTo getOrElse SeqNr.Min
          val eventualRecords = for {
            (record, partitionOffset) <- records
            action <- PartialFunction.condOpt(record.action) { case a: Action.Append => a }.toIterable
            if action.range.to > deleteTo
            event <- EventsSerializer.fromBytes(action.events).toList
            if event.seqNr > deleteTo
          } yield {
            EventualRecord(
              id = record.id,
              seqNr = event.seqNr,
              timestamp = action.timestamp,
              payload = event.payload,
              tags = event.tags,
              partitionOffset = partitionOffset)
          }

          val updateTmp = UpdateTmp.DeleteToKnown(batch.deleteTo, eventualRecords.toVector)

          for {
            result <- db.save(id, updateTmp, topic)
          } yield {
            val head = eventualRecords.head
            val last = eventualRecords.last
            val range = SeqRange(head.seqNr, last.seqNr)
            val offset = last.partitionOffset.offset
            val deleteTo = batch.deleteTo
            log.info(s"replicate id: $id, range: $range, deleteTo: $deleteTo, offset: $offset")
            result
          }
        }

        def onDelete(batch: ActionBatch.DeleteTo) = {
          val deleteTo = batch.seqNr
          val updateTmp = UpdateTmp.DeleteUnbound(deleteTo)
          for {
            result <- db.save(id, updateTmp, topic)
          } yield {
            log.info(s"replicate id: $id, deleteTo: $deleteTo")
            result
          }
        }

        val batch = ActionBatch(actions)
        batch match {
          case batch: ActionBatch.NonEmpty => onNonEmpty(batch)
          case batch: ActionBatch.DeleteTo => onDelete(batch)
          case ActionBatch.Empty           => Future.unit
        }
      }

      val pointers = consumerRecords.values.map { case (topicPartition, records) =>
        val offset = records.foldLeft[Offset](0L /*TODO what if empty ?*/) { (offset, record) => record.offset max offset }
        (topicPartition, offset)
      }

      val timestamp = Instant.now() // TODO move upppp

      def save() = {
        val tmp = for {
          (topicPartition, offset) <- pointers
        } yield {
          val created = if (state.pointers contains topicPartition) None else Some(timestamp)
          (topicPartition, (offset, created))
        }
        val updatePointers = UpdatePointers(timestamp, tmp)
        db.savePointers(updatePointers)
      }

      for {
        _ <- Future.sequence(futures)
        _ <- save()
      } yield {
        val str = pointers.map { case (topicPartition, offset) =>
          val topic = topicPartition.topic
          val partition = topicPartition.partition

          s"\t\t\ttopic: $topic, partition: $partition, offset: $offset"
        } mkString "\n"

        println(s"Replicator pointers:\n$str")
        state.copy(state.pointers ++ pointers)
      }
    }

    def state() = {
      for {
        topicPointers <- db.topicPointers(topic)
      } yield {
        val pointers = for {
          (partition, offset) <- topicPointers.pointers
        } yield {
          val topicPartition = TopicPartition(topic, partition)
          (topicPartition, offset)
        }
        State(pointers)
      }
    }

    // TODO cache state and not re-read it when kafka is broken
    def consume(state: State) = consumer.foldAsync(state, pollTimeout) { (state, records) =>
      shutdown match {
        case Some(shutdown) =>
          val future = consumer.close(closeTimeout)
          shutdown.completeWith(future)

          for {
            _ <- future.recover { case _ => () }
          } yield {
            (state, false)
          }

        case None =>

          if (records.values.nonEmpty) {
            for {
              state <- apply(state, records)
            } yield {
              (state, true)
            }
          } else {
            val result = (state, true)
            Future.successful(result)
          }
      }
    }

    val future = for {
      state <- state()
      _ <- consume(state)
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
