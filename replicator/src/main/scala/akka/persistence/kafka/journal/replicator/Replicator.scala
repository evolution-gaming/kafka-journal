package akka.persistence.kafka.journal.replicator

import java.time.Instant
import java.util.UUID

import akka.actor.ActorSystem
import com.evolutiongaming.cassandra.{CassandraConfig, CreateCluster}
import com.evolutiongaming.kafka.journal.Alias.SeqNr
import com.evolutiongaming.kafka.journal.ally.cassandra.{AllyCassandra, AllyCassandraConfig, SchemaConfig}
import com.evolutiongaming.kafka.journal.ally.{AllyDb, AllyRecord, PartitionOffset}
import com.evolutiongaming.kafka.journal.{Action, EventsSerializer, JournalRecord, SeqRange}
import com.evolutiongaming.skafka.{Bytes, Offset, Partition, Topic}
import com.evolutiongaming.skafka.consumer._
import com.evolutiongaming.kafka.journal.ActionConverters._
import play.api.libs.json.Json

import scala.collection.immutable.Seq
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future, Promise}
import scala.util.control.NonFatal


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
    val config = AllyCassandraConfig.Default
    val allyDb = AllyCassandra(session, schemaConfig, config)
    apply(consumer, allyDb)
  }

  def apply(
    consumer: Consumer[String, Bytes],
    allyDb: AllyDb,
    pollTimeout: FiniteDuration = 100.millis,
    closeTimeout: FiniteDuration = 10.seconds)(implicit ec: ExecutionContext): Shutdown = {

    //    val executorService = Executors.newSingleThreadExecutor()
    //    val executionContext = ExecutionContext.fromExecutor(executorService)

    val topics = List("journal")
    consumer.subscribe(topics)

    val ecBlocking = ec // TODO

    // TODO replace with StateVar
    @volatile var shutdown = Option.empty[Promise[Unit]]


     case class State(topics: Map[Topic, Map[Partition, Offset]])


    // TODO cache state and not re-read it when kafka is broken
    def poll(): Unit = {

      shutdown match {
        case Some(shutdown) =>
          val future = Future {
            consumer.close(closeTimeout)
          }(ecBlocking)

          shutdown.completeWith(future)

        case None =>

          val records = Await.result(consumer.poll(pollTimeout), 3.seconds)

          try {

            val futures = for {
              records <- records.values.values
              (key, records) <- records.groupBy { _.key }
              id <- key
            } yield {

              val topic = records.head.topic

              // TODO handle use case when the `Mark` is first ever Action
              case class Result(deleteTo: SeqNr, xs: Seq[AllyRecord])

              def toAllyRecord(record: ConsumerRecord[String, Bytes], event: JournalRecord.Event) = {
                // TODO different created and inserted timestamps

                val seqNr = event.seqNr

                val timestamp = Instant.now()

                AllyRecord(
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
                    val records = events.map { event => toAllyRecord(record, event) }
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
//                                allyDb.dele
//                ???
              }

              val allyRecords = result.xs
              val future = {
                if (allyRecords.nonEmpty) {
                  allyDb.save(allyRecords, topic).map { result =>
                    val head = allyRecords.head
                    val last = allyRecords.last
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

            val future = Future.sequence(futures)
            Await.result(future, 1.minute)

            val pointers = records.values.map { case (topicPartition, records) =>
              val offset = records.foldLeft(0L) { (offset, record) => record.offset max offset    }
              (topicPartition, offset)
            }
            

            
          } catch {
            case NonFatal(failure) => failure.printStackTrace()
          }

          poll()
      }
    }

    val state = State(Map.empty)

    val future = Future {
      poll()
    }

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
