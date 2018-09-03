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
import com.evolutiongaming.kafka.journal.{Bytes, Journals, SeqNr, SeqRange}
import com.evolutiongaming.safeakka.actor.ActorLog
import com.evolutiongaming.serialization.{SerializedMsgConverter, SerializedMsgExt}
import com.evolutiongaming.skafka.Topic
import com.evolutiongaming.skafka.consumer.{ConsumerConfig, CreateConsumer}
import com.evolutiongaming.skafka.producer.{CreateProducer, ProducerConfig}
import com.typesafe.config.Config

import scala.collection.immutable.Seq
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContextExecutor, Future}
import scala.util.control.NonFatal

class KafkaJournal(config: Config) extends AsyncWriteJournal {

  implicit val system: ActorSystem = context.system
  implicit val ec: ExecutionContextExecutor = system.dispatcher

  val log: ActorLog = ActorLog(system, classOf[KafkaJournal])

  val adapter: JournalsAdapter = adapterOf(toKey(), serialisation())

  def serialisation(): SerializedMsgConverter = SerializedMsgExt(system)

  def toKey(): ToKey = ToKey(config)

  def kafkaConfig(name: String): Config = {
    val kafka = config.getConfig("kafka")
    kafka.getConfig(name) withFallback kafka
  }

  def producerConfig() = ProducerConfig(kafkaConfig("producer"))

  def consumerConfig() = ConsumerConfig(kafkaConfig("consumer"))

  def cassandraConfig(): EventualCassandraConfig = {
    config.getOpt[Config]("cassandra") match {
      case Some(config) => EventualCassandraConfig(config)
      case None         => EventualCassandraConfig.Default
    }
  }

  def adapterOf(toKey: ToKey, serialisation: SerializedMsgConverter): JournalsAdapter = {

    val producerConfig = this.producerConfig()
    log.debug(s"Producer config: $producerConfig")

    val ecBlocking = system.dispatchers.lookup("evolutiongaming.kafka-journal.persistence.journal.blocking-dispatcher")

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

    val consumerConfig = this.consumerConfig()
    log.debug(s"Consumer config: $consumerConfig")

    val newConsumer = (topic: Topic) => {
      val uuid = UUID.randomUUID()
      val prefix = consumerConfig.groupId getOrElse "journal"
      val groupId = s"$prefix-$topic-$uuid"
      val configFixed = consumerConfig.copy(groupId = Some(groupId))
      CreateConsumer[String, Bytes](configFixed, ecBlocking)
    }

    val eventualJournal: EventualJournal = {
      val config = cassandraConfig()
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

  def asyncDeleteMessagesTo(persistenceId: PersistenceId, to: Long) = {
    SeqNr.opt(to) match {
      case Some(to) => adapter.delete(persistenceId, to)
      case None     => Future.unit
    }
  }

  def asyncReplayMessages(persistenceId: PersistenceId, from: Long, to: Long, max: Long)(f: PersistentRepr => Unit) = {
    val seqNrFrom = SeqNr(from, SeqNr.Min)
    val seqNrTo = SeqNr(to, SeqNr.Max)
    val range = SeqRange(seqNrFrom, seqNrTo)
    adapter.replay(persistenceId, range, max)(f)
  }

  def asyncReadHighestSequenceNr(persistenceId: PersistenceId, from: Long) = {
    val seqNr = SeqNr(from, SeqNr.Min)
    for {
      seqNr <- adapter.lastSeqNr(persistenceId, seqNr)
    } yield seqNr match {
      case Some(seqNr) => seqNr.value
      case None        => from
    }
  }
}