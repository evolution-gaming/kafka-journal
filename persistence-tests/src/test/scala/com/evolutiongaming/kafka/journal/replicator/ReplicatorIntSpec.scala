package com.evolutiongaming.kafka.journal.replicator

import java.time.Instant
import java.util.UUID

import com.evolutiongaming.cassandra.CreateCluster
import com.evolutiongaming.concurrent.FutureHelper._
import com.evolutiongaming.concurrent.async.Async
import com.evolutiongaming.kafka.journal.FixEquality.Implicits._
import com.evolutiongaming.kafka.journal.FoldWhileHelper.Switch
import com.evolutiongaming.kafka.journal._
import com.evolutiongaming.kafka.journal.eventual.EventualJournal
import com.evolutiongaming.kafka.journal.eventual.cassandra.{EventualCassandra, EventualCassandraConfig}
import com.evolutiongaming.nel.Nel
import com.evolutiongaming.safeakka.actor.ActorLog
import com.evolutiongaming.skafka.Offset
import com.evolutiongaming.skafka.producer.Producer
import com.typesafe.config.{Config, ConfigFactory}
import org.scalatest.{Matchers, WordSpec}

import scala.concurrent.Await
import scala.concurrent.duration._

class ReplicatorIntSpec extends WordSpec with ActorSpec with Matchers {

  lazy val conf = system.settings.config.getConfig("evolutiongaming.kafka-journal.replicator")

  implicit lazy val ec = system.dispatcher

  lazy val log = ActorLog(system, getClass)

  val timeout = 30.seconds

  lazy val (eventual, session, cassandra) = {
    val config = EventualCassandraConfig(conf.getConfig("cassandra"))
    val cassandra = CreateCluster(config.client)
    val session = Await.result(cassandra.connect(), timeout)
    // TODO add EventualCassandra.close and simplify all
    val eventual = EventualCassandra(session, config, Log.empty(Async.unit))
    (EventualJournal(eventual, log), session, cassandra)
  }

  override def configOf(): Config = ConfigFactory.load("replicator.conf")

  override def beforeAll() = {
    super.beforeAll()
    IntegrationSuit.start()
    eventual
  }

  override def afterAll() = {
    Safe {
      Await.result(session.close(), timeout)
    }
    Safe {
      Await.result(cassandra.close(), timeout)
    }
    super.afterAll()
  }


  "Replicator" should {

    implicit val fixEquality = FixEquality.array[Byte]()

    val topic = "journal"
    val origin = Origin("replicator")

    lazy val journal = {
      val journalConfig = JournalConfig(conf)
      val producer = Producer(journalConfig.producer, ec)

      // TODO we don't need consumer here...
      val topicConsumer = TopicConsumer(journalConfig.consumer, ec, "replicator", None)

      val journal = Journal(
        log = log, // TODO remove
        Some(origin),
        producer = producer,
        topicConsumer = topicConsumer,
        eventual = eventual,
        pollTimeout = 100.millis /*TODO*/ ,
        closeTimeout = timeout/*TODO*/)
      Journal(journal, log)
    }

    def read(key: Key)(until: List[ReplicatedEvent] => Boolean) = {
      val future = Retry() {
        for {
          switch <- eventual.read[List[ReplicatedEvent]](key, SeqNr.Min, Nil) { case (xs, x) => Switch.continue(x :: xs) }.future
          events = switch.s
          result <- if (until(events)) Some(events.reverse).future else None.future
        } yield result
      }

      Await.result(future, timeout)
    }

    def append(key: Key, events: Nel[Event]) = {
      val timestamp = Instant.now()
      val partitionOffset = journal.append(key, events, timestamp).get(timeout)
      for {
        event <- events
      } yield {
        ReplicatedEvent(event, timestamp, partitionOffset, Some(origin))
      }
    }

    def lastSeqNr(key: Key) = journal.lastSeqNr(key, SeqNr.Min).get(timeout)

    def topicPointers() = eventual.pointers(topic).get(timeout).values

    "replicate events and then delete" in {

      val key = Key(id = UUID.randomUUID().toString, topic = topic)

      lastSeqNr(key) shouldEqual None

      val pointers = topicPointers()

      val expected1 = append(key, Nel(event(1)))
      val partitionOffset = expected1.head.partitionOffset
      val partition = partitionOffset.partition

      for {
        offset <- pointers.get(partitionOffset.partition)
      } partitionOffset.offset should be > offset

      val actual1 = read(key)(_.nonEmpty)
      actual1 shouldEqual expected1.toList
      lastSeqNr(key) shouldEqual Some(expected1.last.seqNr)

      journal.delete(key, expected1.last.event.seqNr, Instant.now()).get(timeout).map(_.partition) shouldEqual Some(partition)
      read(key)(_.isEmpty) shouldEqual Nil
      lastSeqNr(key) shouldEqual Some(expected1.last.seqNr)

      val expected2 = append(key, Nel(event(2), event(3)))
      val actual2 = read(key)(_.nonEmpty)
      actual2 shouldEqual expected2.toList
      lastSeqNr(key) shouldEqual Some(expected2.last.seqNr)
    }

    val numberOfEvents = 100

    s"replicate append of $numberOfEvents events" in {
      val key = Key(id = UUID.randomUUID().toString, topic = topic)
      val events = for {
        seqNr <- 1 to numberOfEvents
      } yield {
        event(seqNr, Payload("kafka-journal"))
      }
      val expected = append(key, Nel.unsafe(events))
      val actual = read(key)(_.nonEmpty)
      actual.fix shouldEqual expected.toList.fix

      lastSeqNr(key) shouldEqual Some(events.last.seqNr)
    }

    for {
      (name, events) <- List(
        ("empty", Nel(event(1))),
        ("binary", Nel(event(1, Payload.Binary("binary")))),
        ("text", Nel(event(1, Payload.Text("text")))),
        ("json", Nel(event(1, Payload.Json("json")))),
        ("empty-many", Nel(
          event(1),
          event(2),
          event(3))),
        ("binary-many", Nel(
          event(1, Payload.Binary("1")),
          event(2, Payload.Binary("2")),
          event(3, Payload.Binary("3")))),
        ("text-many", Nel(
          event(1, Payload.Text("1")),
          event(2, Payload.Text("2")),
          event(3, Payload.Text("3")))),
        ("json-many", Nel(
          event(1, Payload.Json("1")),
          event(2, Payload.Json("2")),
          event(3, Payload.Json("3")))),
        ("empty-binary-text-json", Nel(
          event(1),
          event(2, Payload.Binary("binary")),
          event(3, Payload.Text("text")),
          event(4, Payload.Json("json")))))
    } {
      s"consume event from kafka and replicate to eventual journal, payload: $name" in {
        val key = Key(id = UUID.randomUUID().toString, topic = topic)
        val pointers = topicPointers()
        val expected = append(key, events)
        val partition = expected.head.partitionOffset.partition
        val offsetBefore = pointers.getOrElse(partition, Offset.Min)
        val actual = read(key)(_.nonEmpty)
        actual.fix shouldEqual expected.toList.fix

        lastSeqNr(key) shouldEqual Some(events.last.seqNr)

        val offsetAfter = topicPointers().getOrElse(partition, Offset.Min)
        offsetAfter should be > offsetBefore
      }
    }
  }

  private def event(seqNr: Int, payload: Option[Payload] = None): Event = {
    val tags = (0 to seqNr).map(_.toString).toSet
    Event(SeqNr(seqNr.toLong), tags, payload)
  }

  private def event(seqNr: Int, payload: Payload): Event = {
    event(seqNr, Some(payload))
  }
}
