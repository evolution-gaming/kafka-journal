package com.evolutiongaming.kafka.journal

import java.time.Instant
import java.util.UUID

import akka.persistence.kafka.journal.KafkaJournalConfig
import com.evolutiongaming.cassandra.CreateCluster
import com.evolutiongaming.concurrent.async.Async
import com.evolutiongaming.kafka.journal.FoldWhileHelper.Switch
import com.evolutiongaming.kafka.journal.eventual.cassandra.EventualCassandra
import com.evolutiongaming.nel.Nel
import com.evolutiongaming.safeakka.actor.ActorLog
import com.evolutiongaming.skafka.producer.Producer
import org.scalatest.{Matchers, WordSpec}

import scala.concurrent.Await
import scala.concurrent.duration._

class JournalIntSpec extends WordSpec with ActorSpec with Matchers {

  implicit lazy val ec = system.dispatcher

  lazy val log = ActorLog(system, getClass)

  val timeout = 30.seconds

  val origin = Origin("JournalIntSpec")

  lazy val config = {
    val config = system.settings.config.getConfig("evolutiongaming.kafka-journal.persistence.journal")
    KafkaJournalConfig(config)
  }

  lazy val (journal, cassandra) = {
    val (eventual, cassandra) = {
      val cassandraConfig = config.cassandra
      val cassandra = CreateCluster(cassandraConfig.client)
      val session = Await.result(cassandra.connect(), config.connectTimeout)
      val eventual = EventualCassandra(session, cassandraConfig, Log.empty(Async.unit))
      (eventual, cassandra)
    }
    val ecBlocking = system.dispatchers.lookup(config.blockingDispatcher)
    val producer = Producer(config.journal.producer, ecBlocking)
    val topicConsumer = TopicConsumer(config.journal.consumer, ecBlocking)
    val journal = Journal(producer, Some(origin), topicConsumer, eventual)

    (journal, cassandra)
  }

  override def beforeAll() = {
    super.beforeAll()
    IntegrationSuit.start()
    journal
  }

  override def afterAll() = {
    Safe {
      Await.result(cassandra.close(), config.stopTimeout)
    }
    super.afterAll()
  }

  "Journal" should {

    def read(key: Key) = {
      journal.read[List[Event]](key, SeqNr.Min, Nil) { (xs, x) => Switch.continue(x :: xs) }.get(timeout)
    }

    def lastSeqNr(key: Key) = journal.lastSeqNr(key, SeqNr.Min).get(timeout)

    def delete(key: Key) = journal.delete(key, SeqNr.Max, Instant.now()).get(timeout)

    def keyOf() = Key(id = UUID.randomUUID().toString, topic = "journal")

    def append(key: Key, events: Nel[Event]) = {
      journal.append(key, events, Instant.now()).get(timeout)
    }

    for {
      seqNr <- List(SeqNr.Min, SeqNr(10))
    } {
      s"append, delete, read, lastSeqNr, seqNr: $seqNr" in {
        val key = keyOf()
        lastSeqNr(key) shouldEqual None
        read(key) shouldEqual Nil
        delete(key) shouldEqual None
        val event = Event(seqNr)
        val partition = append(key, Nel(event)).partition
        read(key) shouldEqual List(event)
        delete(key).map(_.partition) shouldEqual Some(partition)
        lastSeqNr(key) shouldEqual Some(seqNr)
        read(key) shouldEqual Nil
      }
    }
  }
}
