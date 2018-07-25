package akka.persistence.kafka.journal

import java.util.UUID

import akka.actor.ActorSystem
import akka.persistence.journal.AsyncWriteJournal
import akka.persistence.{AtomicWrite, PersistentRepr}
import com.evolutiongaming.cassandra.CreateCluster
import com.evolutiongaming.config.ConfigHelper._
import com.evolutiongaming.kafka.journal.Alias._
import com.evolutiongaming.kafka.journal.KafkaConverters._
import com.evolutiongaming.kafka.journal.eventual.EventualJournal
import com.evolutiongaming.kafka.journal.eventual.cassandra.{EventualCassandra, EventualCassandraConfig}
import com.evolutiongaming.kafka.journal.{Bytes, Journals}
import com.evolutiongaming.safeakka.actor.ActorLog
import com.evolutiongaming.serialization.{SerializedMsgConverter, SerializedMsgExt}
import com.evolutiongaming.skafka.consumer.{AutoOffsetReset, ConsumerConfig, CreateConsumer}
import com.evolutiongaming.skafka.producer.{CreateProducer, ProducerConfig}
import com.typesafe.config.Config

import scala.collection.immutable.Seq
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContextExecutor}
import scala.util.control.NonFatal

class PersistenceJournal(config: Config) extends AsyncWriteJournal {

  implicit val system: ActorSystem = context.system
  implicit val ec: ExecutionContextExecutor = system.dispatcher

  val adapter: JournalsAdapter = adapterNew(toKey(), serialisation())

  def serialisation(): SerializedMsgConverter = SerializedMsgExt(system)

  def toKey(): ToKey = ToKey(config)

  def adapterNew(toKey: ToKey, serialisation: SerializedMsgConverter): JournalsAdapter = {

    def kafkaConfig(name: String) = {
      val kafka = config.getConfig("kafka")
      kafka.getConfig(name) withFallback kafka
    }

    val log = ActorLog(system, classOf[PersistenceJournal])

    val producerConfig = ProducerConfig(kafkaConfig("producer"))
    log.debug(s"Producer config: $producerConfig")

    val ecBlocking = system.dispatchers.lookup("kafka.persistence.journal.blocking-dispatcher")

    // TODO use different constructor
    val producer = CreateProducer(producerConfig, ecBlocking)

    val closeTimeout = 10.seconds // TODO from config
    val connectTimeout = 5.seconds // TODO from config

    system.registerOnTermination {
      val future = for {
        _ <- producer.flush()
        _ <- producer.close(closeTimeout)
      } yield ()
      try Await.result(future, closeTimeout) catch {
        case NonFatal(failure) => log.error(s"failed to shutdown producer $failure", failure)
      }
    }

    val consumerConfig = ConsumerConfig(kafkaConfig("consumer"))
    log.debug(s"Consumer config: $consumerConfig")

    val newConsumer = () => {
      val groupId = UUID.randomUUID().toString
      val configFixed = consumerConfig.copy(
        groupId = Some(groupId),
        autoOffsetReset = AutoOffsetReset.Earliest)
      CreateConsumer[String, Bytes](configFixed, ecBlocking)
    }

    val eventualJournal: EventualJournal = {
      val config = this.config.getOpt[Config]("cassandra").fold(EventualCassandraConfig.Default)(EventualCassandraConfig.apply)
      val cluster = CreateCluster(config.client)
      val session = Await.result(cluster.connect(), connectTimeout) // TODO handle this properly
      system.registerOnTermination {
        val result = for {
          _ <- session.close()
          _ <- cluster.close()
        } yield {}
        try {
          Await.result(result, closeTimeout)
        } catch {
          case NonFatal(failure) => log.error(s"failed to shutdown cassandra $failure", failure)
        }
      }
      // TODO read only cassandra statements

      {
        val log = ActorLog(system, EventualCassandra.getClass)
        val eventualJournal = EventualCassandra(session, config, log)
        EventualJournal(eventualJournal, log)
      }
    }

    val journals = Journals(producer, newConsumer, eventualJournal)
    JournalsAdapter(log, toKey, journals, serialisation)
  }

  // TODO optimise concurrent calls asyncReplayMessages & asyncReadHighestSequenceNr for the same persistenceId
  def asyncWriteMessages(atomicWrites: Seq[AtomicWrite]) = {
    adapter.write(atomicWrites)
  }

  def asyncDeleteMessagesTo(persistenceId: PersistenceId, to: SeqNr) = {
    adapter.delete(persistenceId, to)
  }

  def asyncReplayMessages(persistenceId: PersistenceId, from: SeqNr, to: SeqNr, max: Long)(f: PersistentRepr => Unit) = {
    adapter.replay(persistenceId, from, to, max)(f)
  }

  def asyncReadHighestSequenceNr(persistenceId: PersistenceId, from: SeqNr) = {
    adapter.highestSeqNr(persistenceId, from)
  }
}