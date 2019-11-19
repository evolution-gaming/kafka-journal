package com.evolutiongaming.kafka.journal.eventual

import java.time.Instant

import cats.data.{NonEmptyList => Nel, NonEmptyMap => Nem}
import cats.implicits._
import cats.{Applicative, ~>}
import com.evolutiongaming.catshelper.BracketThrowable
import com.evolutiongaming.kafka.journal._
import com.evolutiongaming.skafka.{Offset, Partition, Topic}

import scala.concurrent.duration.FiniteDuration


trait ReplicatedJournalOld[F[_]] {

  def topics: F[List[Topic]]

  def pointers(topic: Topic): F[TopicPointers]

  def append(
    key: Key,
    partitionOffset: PartitionOffset,
    timestamp: Instant,
    expireAfter: Option[FiniteDuration],
    events: Nel[EventRecord]
  ): F[Unit]

  def delete(
    key: Key,
    partitionOffset: PartitionOffset,
    timestamp: Instant,
    deleteTo: SeqNr,
    origin: Option[Origin]
  ): F[Unit]

  def save(topic: Topic, pointers: Nem[Partition, Offset], timestamp: Instant): F[Unit]
}

object ReplicatedJournalOld {

  def apply[F[_] : BracketThrowable](replicatedJournal: ReplicatedJournal[F]): ReplicatedJournalOld[F] = {
    new ReplicatedJournalOld[F] {

      def topics = replicatedJournal.topics

      def pointers(topic: Topic) = {
        replicatedJournal
          .journal(topic)
          .use { _.pointers }
      }

      def append(
        key: Key,
        partitionOffset: PartitionOffset,
        timestamp: Instant,
        expireAfter: Option[FiniteDuration],
        events: Nel[EventRecord]
      ) = {
        replicatedJournal
          .journal(key.topic)
          .use { journal =>
            journal
              .journal(key.id)
              .use { _.append(partitionOffset, timestamp, expireAfter, events) }
          }
      }

      def delete(
        key: Key,
        partitionOffset: PartitionOffset,
        timestamp: Instant,
        deleteTo: SeqNr,
        origin: Option[Origin]
      ) = {
        replicatedJournal
          .journal(key.topic)
          .use { journal =>
            journal
              .journal(key.id)
              .use { _.delete(partitionOffset, timestamp, deleteTo, origin) }
          }
      }

      def save(topic: Topic, pointers: Nem[Partition, Offset], timestamp: Instant) = {
        replicatedJournal
          .journal(topic)
          .use { _.save(pointers, timestamp) }
      }
    }
  }


  def empty[F[_] : Applicative]: ReplicatedJournalOld[F] = new ReplicatedJournalOld[F] {

    def topics = List.empty[Topic].pure[F]

    def pointers(topic: Topic) = TopicPointers.empty.pure[F]

    def append(
      key: Key,
      partitionOffset: PartitionOffset,
      timestamp: Instant,
      expireAfter: Option[FiniteDuration],
      events: Nel[EventRecord]
    ) = ().pure[F]

    def delete(
      key: Key,
      partitionOffset: PartitionOffset,
      timestamp: Instant,
      deleteTo: SeqNr,
      origin: Option[Origin]
    ) = ().pure[F]

    def save(topic: Topic, pointers: Nem[Partition, Offset], timestamp: Instant) = ().pure[F]
  }


  implicit class ReplicatedJournalOps[F[_]](val self: ReplicatedJournalOld[F]) extends AnyVal {

    def mapK[G[_]](f: F ~> G): ReplicatedJournalOld[G] = new ReplicatedJournalOld[G] {

      def topics = f(self.topics)

      def pointers(topic: Topic) = f(self.pointers(topic))

      def append(
        key: Key,
        partitionOffset: PartitionOffset,
        timestamp: Instant,
        expireAfter: Option[FiniteDuration],
        events: Nel[EventRecord]
      ) = {
        f(self.append(key, partitionOffset, timestamp, expireAfter, events))
      }

      def delete(
        key: Key,
        partitionOffset: PartitionOffset,
        timestamp: Instant,
        deleteTo: SeqNr,
        origin: Option[Origin]
      ) = {
        f(self.delete(key, partitionOffset, timestamp, deleteTo, origin))
      }

      def save(topic: Topic, pointers: Nem[Partition, Offset], timestamp: Instant) = {
        f(self.save(topic, pointers, timestamp))
      }
    }
  }
}