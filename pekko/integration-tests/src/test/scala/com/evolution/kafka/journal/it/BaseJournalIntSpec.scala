package com.evolution.kafka.journal.it

import cats.Foldable
import cats.data.NonEmptyList as Nel
import cats.effect.*
import cats.implicits.*
import com.evolution.kafka.journal.*
import com.evolution.kafka.journal.ExpireAfter.implicits.*
import com.evolution.kafka.journal.IOSuite.*
import com.evolution.kafka.journal.conversions.{KafkaRead, KafkaWrite}
import com.evolution.kafka.journal.eventual.{EventualJournal, EventualRead}
import com.evolutiongaming.catshelper.ParallelHelper.*
import com.evolutiongaming.retry.{Retry, Strategy}
import org.scalatest.Succeeded
import org.scalatest.freespec.AsyncFreeSpec
import play.api.libs.json.Json

import java.time.Instant
import java.time.temporal.ChronoUnit
import scala.concurrent.duration.*

abstract class BaseJournalIntSpec[A] extends AsyncFreeSpec with JournalSuite {
  import BaseJournalIntSpec.*
  import IntegrationTestInstances.*

  def event(seqNr: SeqNr): Event[A]

  implicit val kafkaRead: KafkaRead[IO, A]
  implicit val kafkaWrite: KafkaWrite[IO, A]
  implicit val eventualRead: EventualRead[IO, A]

  private[this] val eventualCassandra = allocateSuiteScoped(eventualCassandraResource)
  private[this] val journalProducer = allocateSuiteScoped(journalProducerResource)
  private[this] val journalConsumerPool = allocateSuiteScoped(journalConsumerPoolResource)

  private[this] val journalImplsToTest: Vector[TestJournalImpl] = {
    for {
      useHeadCache <- Vector(true, false)
      (eventualJournalImplName, eventualJournalImpl) <- Vector(
        "empty" -> EventualJournal.empty[IO],
        "cassandra" -> eventualCassandra,
      )
    } yield new TestJournalImpl(
      useHeadCache = useHeadCache,
      eventualJournalImplName = eventualJournalImplName,
      eventualJournalImpl = eventualJournalImpl,
    )
  }

  private[this] final class TestJournalImpl(
    val useHeadCache: Boolean,
    val eventualJournalImplName: String,
    val eventualJournalImpl: EventualJournal[IO],
  ) {
    val describe: String = s"[head cache=$useHeadCache, eventual=$eventualJournalImplName]"

    val journalsResource: ResourceIO[Journals[IO]] = {
      for {
        headCache <- makeHeadCacheResource(eventualJournalImpl, useHeadCache = useHeadCache)
      } yield makeJournals(
        producer = journalProducer,
        consumerResource = journalConsumerPool,
        eventualJournal = eventualJournalImpl,
        headCache = headCache,
      )
    }
  }

  "Journal" - {
    journalImplsToTest.foreach { testJournalImpl =>
      s"for impl ${ testJournalImpl.describe }" - {

        val key = Key.random[IO]("journal")

        // msokolov:
        // Journal release logic here is broken, it was broken before my changes, and I'm not sure how to fix it.
        // release is called in the last test case, and it is not properly released if the last test case is
        // skipped.
        lazy val (journals, release) = testJournalImpl.journalsResource
          .allocated
          .unsafeRunSync()

        for {
          seqNr <- List(SeqNr.min, SeqNr.unsafe(2))
        } {

          s"seqNr: $seqNr" - {

            s"append, delete, read, purge, lastSeqNr" in {
              val result = for {
                key <- key
                journal = makeJournalTest(journals(key))
                pointer <- journal.pointer
                _ = pointer shouldEqual None
                events <- journal.read
                _ = events shouldEqual List.empty
                pointer <- journal.delete(DeleteTo.max)
                _ = pointer shouldEqual None
                anEvent = event(seqNr)
                offset <- journal.append(Nel.of(anEvent), recordMetadata, headers)
                record = EventRecord(anEvent, timestamp, offset, origin.some, version.some, recordMetadata, headers)
                partition = offset.partition
                events <- journal.read
                _ = events shouldEqual List(record)
                pointer <- journal.delete(DeleteTo.max)
                _ = pointer.map { _.partition } shouldEqual partition.some
                pointer <- journal.pointer
                _ = pointer shouldEqual seqNr.some
                events <- journal.read
                _ = events shouldEqual List.empty
                pointer <- journal.purge
                _ = pointer.map { _.partition } shouldEqual partition.some
                pointer <- journal.pointer
                _ = pointer shouldEqual none
                events <- journal.read
                _ = events shouldEqual List.empty
                metadata = recordMetadata.withExpireAfter(1.day.toExpireAfter.some)
                offset <- journal.append(Nel.of(anEvent), metadata, headers)
                record = EventRecord(anEvent, timestamp, offset, origin.some, version.some, metadata, headers)
                events <- journal.read
                _ = events shouldEqual List(record)
                pointer <- journal.delete(DeleteTo.max)
                _ = pointer.map { _.partition } shouldEqual partition.some
                pointer <- journal.pointer
                _ = pointer shouldEqual seqNr.some
                pointer <- journal.purge
                _ = pointer.map { _.partition } shouldEqual partition.some
                pointer <- journal.pointer
                _ = pointer shouldEqual none
              } yield Succeeded

              result.run(1.minute)
            }

            val many = 10
            s"append & read $many" in {

              val events = for {
                n <- (0 until many).toList
                seqNr <- seqNr.map[Option](_ + n)
              } yield {
                event(seqNr)
              }

              val result = for {
                key <- key
                journal = makeJournalTest(journals(key))
                read = for {
                  events1 <- journal.read
                  _ = events1.map(_.event) shouldEqual events
                  pointer <- journal.pointer
                  _ = pointer shouldEqual events.lastOption.map(_.seqNr)
                } yield {}
                _ <- journal.append(Nel.fromListUnsafe(events))
                reads = List.fill(10)(read)
                _ <- Foldable[List].fold(reads)
              } yield {}

              result.run(1.minute)
            }

            s"append & read $many in parallel" in {

              val expected = for {
                n <- (0 to 10).toList
                seqNr <- seqNr.map[Option](_ + n)
              } yield event(seqNr)

              val appends = for {
                key <- key
                journal = makeJournalTest(journals(key))
                events <- journal.read
                _ = events shouldEqual Nil
                pointer <- journal.pointer
                _ = pointer shouldEqual None
                _ <- expected.foldMap { event => journal.append(Nel.of(event)).void }
              } yield {
                for {
                  pointer <- journal.pointer
                  _ = pointer shouldEqual expected.lastOption.map(_.seqNr)
                  events <- journal.read
                  _ = events.map(_.event) shouldEqual expected
                } yield {}
              }
              List
                .fill(10)(appends)
                .parSequence
                .flatMap { _.parFold1 }
                .run(1.minute)
            }

            "append duplicates" ignore {

              val seqNrs = {
                val seqNrs = (0 to 2).foldLeft(Nel.of(seqNr)) { (seqNrs, _) =>
                  seqNrs.head.next[Option].fold(seqNrs) { _ :: seqNrs }
                }
                seqNrs.reverse
              }

              val result = for {
                key <- key
                journal = makeJournalTest(journals(key))
                pointer <- journal.pointer
                _ = pointer shouldEqual None
                events <- journal.read
                _ = events shouldEqual Nil
                offset <- journal.delete(DeleteTo.max)
                _ = offset shouldEqual None
                events = seqNrs.map { seqNr => event(seqNr) }
                append = journal.append(events, recordMetadata, headers)
                _ <- append
                _ <- append
                offset <- append
                records = events.map { event =>
                  EventRecord(event, timestamp, offset, origin.some, version.some, recordMetadata, headers)
                }
                partition = offset.partition
                events <- journal.read
                _ = events shouldEqual records.toList
                offset <- journal.delete(seqNrs.last.toDeleteTo)
                _ = offset.map(_.partition) shouldEqual partition.some
                pointer <- journal.pointer
                _ = pointer shouldEqual seqNrs.last.some
                events <- journal.read
                _ = events shouldEqual Nil
              } yield Succeeded

              result.run(1.minute)
            }
          }
        }

        if (testJournalImpl.useHeadCache) {
          "expire records" ignore {
            val result = for {
              key <- key
              journal = journals(key)
              metadata = RecordMetadata(payload = PayloadMetadata(1.second.toExpireAfter.some))
              _ <- journal.append(Nel.of(event(SeqNr.min)), metadata)
              events <- journal.read().toList
              _ = events.map(_.seqNr) shouldEqual List(SeqNr.min)
              strategy = Strategy.const(100.millis).limit(10.seconds)
              retry = Retry[IO, Throwable](strategy)
              _ <- retry {
                for {
                  events <- journal.read().toList
                  _ = events shouldEqual List.empty
                } yield {}
              }
            } yield {}

            result.run(1.minute)
          }
        }

        "release" in {
          release.run(1.minute)
        }
      }
    }

    s"ids" in {
      val result = for {
        ids <- eventualCassandra.ids("journal").toList
        _ <- IO { ids should not be empty }
        result <- IO { ids.distinct shouldEqual ids }
      } yield result
      result.run(1.minute)
    }
  }
}

private[it] object BaseJournalIntSpec {
  import JournalSuite.*

  private val timestamp = Instant.now().truncatedTo(ChronoUnit.MILLIS)
  private val version = Version.current
  private val recordMetadata = RecordMetadata(HeaderMetadata(Json.obj(("key", "value")).some), PayloadMetadata.empty)
  private val headers = Headers(("key", "value"))

  private def makeJournalTest(journal: Journal[IO]): JournalTest =
    new JournalTest(journal, readRecordTimestampOverride = timestamp.some)
}
