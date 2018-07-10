package akka.persistence.kafka.journal.replicator

import java.time.Instant
import java.util.UUID

import akka.actor.ActorSystem
import com.evolutiongaming.cassandra.{CassandraConfig, CreateCluster}
import com.evolutiongaming.kafka.journal.Alias.SeqNr
import com.evolutiongaming.kafka.journal.ConsumerHelper._
import com.evolutiongaming.kafka.journal.KafkaConverters._
import com.evolutiongaming.kafka.journal.eventual._
import com.evolutiongaming.kafka.journal.eventual.cassandra.{EventualCassandraConfig, EventualDbCassandra, SchemaConfig}
import com.evolutiongaming.kafka.journal.{Action, EventsSerializer, SeqRange}
import com.evolutiongaming.skafka.consumer._
import com.evolutiongaming.skafka.{TopicPartition, _}

import scala.collection.immutable.Seq
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
    closeTimeout: FiniteDuration = 10.seconds)(implicit ec: ExecutionContext): Shutdown = {

    //    val executorService = Executors.newSingleThreadExecutor()
    //    val executionContext = ExecutionContext.fromExecutor(executorService)

    val topic = "journal"
    val topics = List(topic)
    consumer.subscribe(topics)

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

        sealed trait DeleteTo

        case class DeleteToKnown(value: SeqNr) extends DeleteTo

        case class DeleteToUnknown(value: SeqNr) extends DeleteTo


        sealed trait Tmp {

        }

        // TODO rename
        object Tmp {
          case object Empty extends Tmp
          case class DeleteToKnown(deletedTo: Option[SeqNr], records: Seq[EventualRecord]) extends Tmp
          case class DeleteToUnknown(deletedTo: SeqNr) extends Tmp
        }


        val result = records.foldLeft[Tmp](Tmp.Empty) { case (result, (record, partitionOffset)) =>

          record.action match {
            case action: Action.Append =>
              val events = EventsSerializer.EventsFromBytes(action.events, topic).events.to[Vector]
              val records = events.map { event =>
                // TODO different created and inserted timestamps
                EventualRecord(
                  id = id,
                  seqNr = event.seqNr,
                  timestamp = action.timestamp,
                  payload = event.payload,
                  tags = Set.empty, // TODO
                  partitionOffset = partitionOffset)
              }

              result match {
                case Tmp.Empty => Tmp.DeleteToKnown(None, records)

                case Tmp.DeleteToKnown(deleteTo, xs) =>
                  Tmp.DeleteToKnown(deleteTo, xs ++ records)

                case Tmp.DeleteToUnknown(value) =>

                  val range = action.header.range
                  if (value < range.from) {
                    Tmp.DeleteToKnown(Some(value), records)
                  } else if (value >= range.to) {
                    Tmp.DeleteToKnown(Some(range.to), Vector.empty)
                  } else {
                    val result = records.dropWhile { _.seqNr <= value }
                    Tmp.DeleteToKnown(Some(value), result)
                  }
              }


            case action: Action.Delete =>

              val deleteTo = action.header.to

              result match {
                case Tmp.Empty => Tmp.DeleteToUnknown(deleteTo)
                // TODO use the same logic in Eventual
                case Tmp.DeleteToKnown(value, records) =>

                  value match {
                    case None =>
                      if (records.isEmpty) {
                        ???
                      } else {
                        val head = records.head.seqNr
                        if (deleteTo < head) {
                          Tmp.DeleteToKnown(None, records)
                        } else {
                          val result = records.dropWhile { _.seqNr <= deleteTo }
                          val lastSeqNr = result.lastOption.fold(records.last.seqNr) { _.seqNr }
                          Tmp.DeleteToKnown(Some(lastSeqNr), result)
                        }
                      }


                    case Some(deleteToPrev) =>
                      if (records.isEmpty) {
                        Tmp.DeleteToKnown(Some(deleteToPrev), records)
                      } else {
                        if (deleteTo <= deleteToPrev) {
                          Tmp.DeleteToKnown(Some(deleteToPrev), records)
                        } else {
                          val result = records.dropWhile { _.seqNr <= deleteTo }
                          val lastSeqNr = result.lastOption.fold(records.last.seqNr) { _ => deleteTo }
                          Tmp.DeleteToKnown(Some(lastSeqNr), result)
                        }
                      }
                  }

                case Tmp.DeleteToUnknown(deleteToPrev) => Tmp.DeleteToUnknown(deleteTo max deleteToPrev)
              }

            case action: Action.Mark => result
          }
        }


        result match {
          case Tmp.Empty => Future.unit

          case Tmp.DeleteToKnown(deletedTo, records) =>

            val updateTmp = UpdateTmp.DeleteToKnown(deletedTo, records)

            db.save(id, updateTmp, topic).map { result =>
              if (records.nonEmpty) {
                val head = records.head
                val last = records.last
                val range = SeqRange(head.seqNr, last.seqNr)
                val offset = last.partitionOffset.offset

                println(s"$id replicate.save range: $range offset: $offset")
              }
              result
            }


          case Tmp.DeleteToUnknown(value) =>
            println(s"$id replicate.save DeleteToUnknown($value)")
            val updateTmp = UpdateTmp.DeleteUnbound(value)
            db.save(id, updateTmp, topic)
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
