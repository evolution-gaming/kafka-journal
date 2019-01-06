package com.evolutiongaming.kafka.journal

import java.time.Instant

import akka.persistence.kafka.journal.KafkaJournalConfig
import cats.implicits._
import cats.effect.{IO, Resource}
import com.evolutiongaming.concurrent.async.Async
import com.evolutiongaming.kafka.journal.FoldWhile._
import com.evolutiongaming.kafka.journal.eventual.cassandra.{CassandraCluster, EventualCassandra}
import com.evolutiongaming.kafka.journal.util.IOSuite._
import com.evolutiongaming.nel.Nel
import org.scalatest.{Matchers, Suite}

import scala.concurrent.duration._
import scala.concurrent.ExecutionContextExecutor

trait JournalSuit extends ActorSuite with Matchers { self: Suite =>

  lazy val config: KafkaJournalConfig = {
    val config = system.settings.config.getConfig("evolutiongaming.kafka-journal.persistence.journal")
    KafkaJournalConfig(config)
  }

  lazy val blocking: ExecutionContextExecutor = system.dispatchers.lookup(config.blockingDispatcher)

  lazy val ((eventual, producer), release) = {
    val resource = for {
      cassandraCluster <- CassandraCluster.of[IO](config.cassandra.client, config.cassandra.retries)
      cassandraSession <- cassandraCluster.session
      eventualJournal  <- {
        implicit val cassandraSession1 = cassandraSession
        Resource.liftF(EventualCassandra.of[IO](config.cassandra, None))
      }
      kafkaProducer <- KafkaProducer.of[IO](config.journal.producer, blocking)
    } yield {
      (eventualJournal, kafkaProducer)
    }

    resource.allocated.unsafeRunSync()
  }

  override def beforeAll() = {
    super.beforeAll()
    IntegrationSuit.start()
//    eventual
//    producer
  }

  override def afterAll() = {
    release.unsafeRunSync()
    super.afterAll()
  }
}

object JournalSuit {

  trait KeyJournal {

    def append(events: Nel[Event]): Async[PartitionOffset]

    def read(): Async[List[Event]]

    def size(): Async[Int]

    def pointer(): Async[Option[SeqNr]]

    def delete(to: SeqNr): Async[Option[PartitionOffset]]
  }

  object KeyJournal {

    def apply(key: Key, journal: Journal[Async]): KeyJournal = new KeyJournal {

      def append(events: Nel[Event]) = {
        journal.append(key, events, Instant.now())
      }

      def read() = {
        for {
          events <- journal.read[List[Event]](key, SeqNr.Min, Nil) { (xs, x) => Switch.continue(x :: xs) }
        } yield {
          events.reverse
        }
      }

      def size() = {
        journal.read[Int](key, SeqNr.Min, 0) { (a, _) => Switch.continue(a + 1) }
      }

      def pointer() = {
        journal.pointer(key)
      }

      def delete(to: SeqNr) = {
        journal.delete(key, to, Instant.now())
      }
    }
  }


  trait KeyJournalSync {

    def append(events: Nel[Event]): PartitionOffset

    def read(): List[Event]

    def size(): Int

    def pointer(): Option[SeqNr]

    def delete(to: SeqNr): Option[PartitionOffset]
  }

  object KeyJournalSync {

    def apply(key: Key, journal: Journal[Async], timeout: FiniteDuration): KeyJournalSync = new KeyJournalSync {

      def append(events: Nel[Event]) = {
        journal.append(key, events, Instant.now()).get(timeout)
      }

      def read() = {
        val events = journal.read[List[Event]](key, SeqNr.Min, Nil) { (xs, x) => Switch.continue(x :: xs) }.get(timeout)
        events.reverse
      }

      def size() = {
        journal.read[Int](key, SeqNr.Min, 0) { (a, _) => Switch.continue(a + 1) }.get(timeout)
      }

      def pointer() = {
        journal.pointer(key).get(timeout)
      }

      def delete(to: SeqNr) = {
        journal.delete(key, to, Instant.now()).get(timeout)
      }
    }
  }
}