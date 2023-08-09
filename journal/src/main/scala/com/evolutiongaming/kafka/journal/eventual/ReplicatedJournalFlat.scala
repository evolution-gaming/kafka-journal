package com.evolutiongaming.kafka.journal.eventual

import java.time.Instant

import cats.data.{NonEmptyList => Nel}
import cats.syntax.all._
import com.evolutiongaming.catshelper.BracketThrowable
import com.evolutiongaming.kafka.journal._
import com.evolutiongaming.kafka.journal.eventual.ReplicatedKeyJournal.Changed
import com.evolutiongaming.skafka.{Offset, Partition, Topic}

import scala.collection.immutable.SortedSet



trait ReplicatedJournalFlat[F[_]] {

  def topics: F[SortedSet[Topic]]

  def offset(topic: Topic, partition: Partition): F[Option[Offset]]

  def offsetCreate(topic: Topic, partition: Partition, offset: Offset, timestamp: Instant): F[Unit]

  def offsetUpdate(topic: Topic, partition: Partition, offset: Offset, timestamp: Instant): F[Unit]

  def append(
    key: Key,
    partition: Partition,
    offset: Offset,
    timestamp: Instant,
    expireAfter: Option[ExpireAfter],
    events: Nel[EventRecord[EventualPayloadAndType]]
  ): F[Changed]

  def delete(
    key: Key,
    partition: Partition,
    offset: Offset,
    timestamp: Instant,
    deleteTo: DeleteTo,
    origin: Option[Origin]
  ): F[Changed]

  def purge(
    key: Key,
    partition: Partition,
    offset: Offset,
    timestamp: Instant
  ): F[Changed]
}

object ReplicatedJournalFlat {

  def apply[F[_] : BracketThrowable](replicatedJournal: ReplicatedJournal[F]): ReplicatedJournalFlat[F] = {
    class Main
    new Main with ReplicatedJournalFlat[F] {

      def topics = replicatedJournal.topics

      def offset(topic: Topic, partition: Partition): F[Option[Offset]] = {
        replicatedJournal
          .journal(topic)
          .use { journal =>
            journal
              .apply(partition)
              .use { journal =>
                journal
                  .offsets
                  .get
              }
          }
      }

      def offsetCreate(topic: Topic, partition: Partition, offset: Offset, timestamp: Instant) = {
        replicatedJournal
          .journal(topic)
          .use { journal =>
            journal
              .apply(partition)
              .use { journal =>
                journal
                  .offsets
                  .create(offset, timestamp)
              }
          }
      }

      def offsetUpdate(topic: Topic, partition: Partition, offset: Offset, timestamp: Instant) = {
        replicatedJournal
          .journal(topic)
          .use { journal =>
            journal
              .apply(partition)
              .use { journal =>
                journal
                  .offsets
                  .update(offset, timestamp)
              }
          }
      }

      def append(
        key: Key,
        partition: Partition,
        offset: Offset,
        timestamp: Instant,
        expireAfter: Option[ExpireAfter],
        events: Nel[EventRecord[EventualPayloadAndType]]
      ) = {
        replicatedJournal
          .journal(key.topic)
          .use { journal =>
            journal
              .apply(partition)
              .use { journal =>
                journal
                  .journal(key.id)
                  .use { _.append(offset, timestamp, expireAfter, events) }
              }
          }
      }

      def delete(
        key: Key,
        partition: Partition,
        offset: Offset,
        timestamp: Instant,
        deleteTo: DeleteTo,
        origin: Option[Origin]
      ) = {
        replicatedJournal
          .journal(key.topic)
          .use { journal =>
            journal
              .apply(partition)
              .use { journal =>
                journal
                  .journal(key.id)
                  .use { _.delete(offset, timestamp, deleteTo, origin) }
              }
          }
      }

      def purge(
        key: Key,
        partition: Partition,
        offset: Offset,
        timestamp: Instant
      ) = {
        replicatedJournal
          .journal(key.topic)
          .use { journal =>
            journal
              .apply(partition)
              .use { journal =>
                journal
                  .journal(key.id)
                  .use { _.purge(offset, timestamp) }
              }
          }
      }
    }
  }
}