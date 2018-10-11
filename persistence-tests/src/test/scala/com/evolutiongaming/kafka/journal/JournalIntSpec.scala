package com.evolutiongaming.kafka.journal

import java.time.Instant
import java.util.UUID

import akka.persistence.kafka.journal.KafkaJournalConfig
import com.evolutiongaming.cassandra.CreateCluster
import com.evolutiongaming.concurrent.async.Async
import com.evolutiongaming.kafka.journal.FoldWhileHelper.Switch
import com.evolutiongaming.kafka.journal.eventual.EventualJournal
import com.evolutiongaming.kafka.journal.eventual.cassandra.EventualCassandra
import com.evolutiongaming.nel.Nel
import com.evolutiongaming.skafka.producer.Producer
import org.scalatest.{Matchers, WordSpec}

import scala.concurrent.Await
import scala.concurrent.duration._

class JournalIntSpec extends WordSpec with ActorSpec with Matchers {
  import JournalIntSpec._

  private implicit lazy val ec = system.dispatcher

  private val timeout = 30.seconds

  val origin = Origin("JournalIntSpec")

  lazy val config = {
    val config = system.settings.config.getConfig("evolutiongaming.kafka-journal.persistence.journal")
    KafkaJournalConfig(config)
  }

  lazy val (eventual, cassandra) = {
    val cassandraConfig = config.cassandra
    val cassandra = CreateCluster(cassandraConfig.client)
    val session = Await.result(cassandra.connect(), config.connectTimeout)
    val eventual = EventualCassandra(session, cassandraConfig, Log.empty(Async.unit))
    (eventual, cassandra)
  }

  lazy val journalOf = {
    val ecBlocking = system.dispatchers.lookup(config.blockingDispatcher)
    val producer = Producer(config.journal.producer, ecBlocking)
    val topicConsumer = TopicConsumer(config.journal.consumer, ecBlocking)
    (eventual: EventualJournal, key: Key) => {
      val journal = Journal(
        producer = producer,
        origin = Some(origin),
        topicConsumer = topicConsumer,
        eventual = eventual,
        pollTimeout = config.journal.pollTimeout,
        closeTimeout = config.journal.closeTimeout)
      KeyJournal(key, journal, timeout)
    }
  }

  override def beforeAll() = {
    super.beforeAll()
    IntegrationSuit.start()
    eventual
  }

  override def afterAll() = {
    Safe {
      Await.result(cassandra.close(), config.stopTimeout)
    }
    super.afterAll()
  }

  "Journal" should {

    for {
      seqNr <- List(SeqNr.Min, SeqNr(10))
      (eventualName, eventual) <- List(
        ("empty", () => eventual),
        ("non-empty", () => EventualJournal.Empty))
    } {
      val name = s"seqNr: $seqNr, eventual: $eventualName"

      def keyOf() = Key(id = UUID.randomUUID().toString, topic = "journal")

      s"append, delete, read, lastSeqNr, $name" in {
        val key = keyOf()
        val journal = journalOf(eventual(), key)
        journal.lastSeqNr() shouldEqual None
        journal.read() shouldEqual Nil
        journal.delete(SeqNr.Max) shouldEqual None
        val event = Event(seqNr)
        val partition = journal.append(Nel(event)).partition
        journal.read() shouldEqual List(event)
        journal.delete(SeqNr.Max).map(_.partition) shouldEqual Some(partition)
        journal.lastSeqNr() shouldEqual Some(seqNr)
        journal.read() shouldEqual Nil
      }

      val many = 100
      s"append & read $many, $name" in {
        val key = keyOf()
        val journal = journalOf(eventual(), key)

        val events = for {
          n <- 0 until many
          seqNr <- seqNr.map(_ + n)
        } yield {
          Event(seqNr)
        }

        journal.append(Nel.unsafe(events))

        Async.foldUnit {
          for {
            _ <- 0 to 10
          } yield Async.async {
            journal.read() shouldEqual events
          }
        }.get(timeout)
      }
    }
  }
}

object JournalIntSpec {

  trait KeyJournal {

    def append(events: Nel[Event]): PartitionOffset

    def read(): List[Event]

    def lastSeqNr(): Option[SeqNr]

    def delete(to: SeqNr): Option[PartitionOffset]
  }

  object KeyJournal {

    def apply(key: Key, journal: Journal, timeout: FiniteDuration): KeyJournal = new KeyJournal {

      def append(events: Nel[Event]) = {
        journal.append(key, events, Instant.now()).get(timeout)
      }

      def read() = {
        val events = journal.read[List[Event]](key, SeqNr.Min, Nil) { (xs, x) => Switch.continue(x :: xs) }.get(timeout)
        events.reverse
      }

      def lastSeqNr() = {
        journal.lastSeqNr(key, SeqNr.Min).get(timeout)
      }

      def delete(to: SeqNr) = {
        journal.delete(key, SeqNr.Max, Instant.now()).get(timeout)
      }
    }
  }
}
