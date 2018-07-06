package akka.persistence.kafka.journal.replicator

import java.time.Instant
import java.util.UUID

import akka.actor.ActorSystem
import com.evolutiongaming.cassandra.{CassandraConfig, CreateCluster}
import com.evolutiongaming.kafka.journal.ActionConverters._
import com.evolutiongaming.kafka.journal.Alias.SeqNr
import com.evolutiongaming.kafka.journal.ConsumerHelper._
import com.evolutiongaming.kafka.journal.eventual.cassandra.{EventualCassandra, EventualCassandraConfig, EventualDbCassandra, SchemaConfig}
import com.evolutiongaming.kafka.journal.eventual.{EventualDb, EventualRecord, PartitionOffset, UpdatePointers}
import com.evolutiongaming.kafka.journal.{Action, EventsSerializer, JournalRecord, SeqRange}
import com.evolutiongaming.skafka.consumer._
import com.evolutiongaming.skafka.{TopicPartition, _}

import scala.collection.immutable.Seq
import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future, Promise}


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
    eventualDb: EventualDb,
    pollTimeout: FiniteDuration = 100.millis,
    closeTimeout: FiniteDuration = 10.seconds)(implicit ec: ExecutionContext): Shutdown = {

    //    val executorService = Executors.newSingleThreadExecutor()
    //    val executionContext = ExecutionContext.fromExecutor(executorService)

    val topic = "journal"
    val topics = List(topic)
    consumer.subscribe(topics)

    // TODO replace with StateVar
    @volatile var shutdown = Option.empty[Promise[Unit]]


    case class State(pointers: Map[TopicPartition, Offset])


    def apply(state: State, records: ConsumerRecords[String, Bytes]): Future[State] = {

      val futures = for {
        records <- records.values.values
        (key, records) <- records.groupBy { _.key }
        id <- key
      } yield {

        val topic = records.head.topic

        // TODO handle use case when the `Mark` is first ever Action
        case class Result(deleteTo: SeqNr, xs: Seq[EventualRecord])

        def toEventualRecord(record: ConsumerRecord[String, Bytes], event: JournalRecord.Event) = {
          // TODO different created and inserted timestamps

          val seqNr = event.seqNr

          val timestamp = Instant.now()

          EventualRecord(
            id = id,
            seqNr = seqNr,
            timestamp = timestamp,
            payload = event.payload,
            tags = Set.empty, // TODO
            partitionOffset = PartitionOffset(
              partition = record.partition,
              offset = record.offset))
        }

        val zero = Result(0, Vector.empty)

        val result: Result = records.foldLeft(zero) { case (result, record) =>

          val action = toAction(record)

          action match {
            case action: Action.Append =>
              val events = EventsSerializer.EventsFromBytes(record.value, topic).events.to[Iterable]
              val records = events.map { event => toEventualRecord(record, event) }
              result.copy(xs = result.xs ++ records)

            case action: Action.Truncate =>
              if (action.to > result.deleteTo) {
                // TODO pasted from Client
                val entries = result.xs.dropWhile { _.seqNr <= action.to }
                result.copy(deleteTo = action.to, xs = entries)
              } else {
                result
              }

            case action: Action.Mark => result
          }
        }


        // TODO commit offset to kafka

        val deleteTo = result.deleteTo

        if (deleteTo == 0) {

        } else {
          println(s"$id replicate.delete deleteTo: $deleteTo")
          //                                eventualDb.dele
          //                ???
        }

        val eventualRecords = result.xs
        val future = {
          if (eventualRecords.nonEmpty) {

            // TODO limit the save size

            eventualDb.save(eventualRecords, topic).map { result =>
              val head = eventualRecords.head
              val last = eventualRecords.last
              val range = SeqRange(head.seqNr, last.seqNr)
              val offset = last.partitionOffset.offset

              println(s"$id replicate.save range: $range offset: $offset")
              result
            }
          } else {
            Future.unit
          }
        }

        future
      }

      val pointers = records.values.map { case (topicPartition, records) =>
        val offset = records.foldLeft[Offset](0L) { (offset, record) => record.offset max offset }
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
        eventualDb.savePointers(updatePointers)
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
        topicPointers <- eventualDb.topicPointers(topic)
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
