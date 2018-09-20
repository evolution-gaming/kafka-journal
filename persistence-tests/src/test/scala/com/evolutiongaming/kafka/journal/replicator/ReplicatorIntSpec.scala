package com.evolutiongaming.kafka.journal.replicator

import java.time.Instant
import java.util.UUID

import com.evolutiongaming.cassandra.CreateCluster
import com.evolutiongaming.concurrent.FutureHelper._
import com.evolutiongaming.kafka.journal.FoldWhileHelper.Switch
import com.evolutiongaming.kafka.journal._
import com.evolutiongaming.kafka.journal.eventual.EventualJournal
import com.evolutiongaming.kafka.journal.eventual.cassandra.{EventualCassandra, EventualCassandraConfig}
import com.evolutiongaming.nel.Nel
import com.evolutiongaming.safeakka.actor.ActorLog
import com.evolutiongaming.skafka.Topic
import com.evolutiongaming.skafka.consumer.{Consumer, ConsumerConfig}
import com.evolutiongaming.skafka.producer.{Producer, ProducerConfig}
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
    val eventual = EventualCassandra(session, config)
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

    "consume event from kafka and store in replicated journal" in {
      val topic = "journal"
      val key = Key(id = UUID.randomUUID().toString, topic = topic)
      val origin = Origin(s"replicator")

      def kafkaConf(name: String) = {
        val common = conf.getConfig("kafka")
        common.getConfig(name) withFallback common
      }

      val journal = {
        val producer = {
          val producerConfig = ProducerConfig(kafkaConf("producer"))
          Producer(producerConfig, ec)
        }
        // TODO refactor journal to reuse code

        // TODO we don't need consumer here...
        val consumerConfig = ConsumerConfig(kafkaConf("consumer"))
        val newConsumer = (topic: Topic) => {
          val uuid = UUID.randomUUID()
          val prefix = consumerConfig.groupId getOrElse "replicator-test"
          val groupId = s"$prefix-$topic-$uuid"
          val configFixed = consumerConfig.copy(groupId = Some(groupId))
          Consumer[String, Bytes](configFixed, ec)
        }

        val journal = Journal(
          log = log, // TODO remove
          Some(origin),
          producer = producer,
          newConsumer = newConsumer,
          eventual = eventual,
          pollTimeout = 100.millis /*TODO*/ ,
          closeTimeout = timeout)
        Journal(journal, log)
      }


      def readUntil(until: List[ReplicatedEvent] => Boolean) = {
        val future = Retry() {
          for {
            switch <- eventual.read[List[ReplicatedEvent]](key, SeqNr.Min, Nil) { case (xs, x) => Switch.continue(x :: xs) }.future
            events = switch.s
            result <- if (until(events)) Some(events.reverse).future else None.future
          } yield result
        }

        Await.result(future, timeout)
      }

      def append(events: Nel[Event]) = {
        val timestamp = Instant.now()
        val partitionOffset = journal.append(key, events, timestamp).get(timeout)
        for {
          event <- events
        } yield {
          ReplicatedEvent(event, timestamp, partitionOffset, Some(origin))
        }
      }

      val topicPointers = eventual.pointers(topic).get(timeout)

      val expected1 = append(Nel(Event(SeqNr.Min)))
      val partitionOffset = expected1.head.partitionOffset
      val partition = partitionOffset.partition

      for {
        offset <- topicPointers.values.get(partitionOffset.partition)
      } partitionOffset.offset should be > offset

      val actual1 = readUntil(_.nonEmpty)
      actual1 shouldEqual expected1.toList

      journal.delete(key, expected1.last.event.seqNr, Instant.now()).get(timeout).partition shouldEqual partition
      readUntil(_.isEmpty) shouldEqual Nil

      val expected2 = append(Nel(
        Event(SeqNr(2l), Set("tag-2")),
        Event(SeqNr(3l), Set("tag-3", "tag-4"))))
      val actual2 = readUntil(_.nonEmpty)
      actual2 shouldEqual expected2.toList
    }
  }
}
