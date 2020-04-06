package com.evolutiongaming.kafka.journal.eventual

import java.time.Instant

import cats.data.{NonEmptyList => Nel, NonEmptyMap => Nem}
import cats.implicits._
import cats.{Applicative, ~>}
import com.evolutiongaming.catshelper.BracketThrowable
import com.evolutiongaming.kafka.journal._
import com.evolutiongaming.kafka.journal.eventual.ReplicatedJournalFlat.Changed
import com.evolutiongaming.skafka.{Offset, Partition, Topic}

import scala.collection.immutable.SortedSet



trait ReplicatedJournalFlat[F[_]] {

  def topics: F[SortedSet[Topic]]

  def pointers(topic: Topic): F[TopicPointers]

  def append[A](
    key: Key,
    partitionOffset: PartitionOffset,
    timestamp: Instant,
    expireAfter: Option[ExpireAfter],
    events: Nel[EventRecord[A]]
  )(implicit W: EventualWrite[F, A]): F[Changed]

  def delete(
    key: Key,
    partitionOffset: PartitionOffset,
    timestamp: Instant,
    deleteTo: DeleteTo,
    origin: Option[Origin]
  ): F[Changed]

  def purge(
    key: Key,
    offset: Offset,
    timestamp: Instant
  ): F[Changed]

  def save(
    topic: Topic,
    pointers: Nem[Partition, Offset],
    timestamp: Instant
  ): F[Changed]
}

object ReplicatedJournalFlat {

  type Changed = Boolean

  def apply[F[_] : BracketThrowable](replicatedJournal: ReplicatedJournal[F]): ReplicatedJournalFlat[F] = {
    new ReplicatedJournalFlat[F] {

      def topics = replicatedJournal.topics

      def pointers(topic: Topic) = {
        replicatedJournal
          .journal(topic)
          .use { _.pointers }
      }

      def append[A](
        key: Key,
        partitionOffset: PartitionOffset,
        timestamp: Instant,
        expireAfter: Option[ExpireAfter],
        events: Nel[EventRecord[A]]
      )(implicit W: EventualWrite[F, A]) = {
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
        deleteTo: DeleteTo,
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

      def purge(
        key: Key,
        offset: Offset,
        timestamp: Instant
      ) = {
        replicatedJournal
          .journal(key.topic)
          .use { journal =>
            journal
              .journal(key.id)
              .use { _.purge(offset, timestamp) }
          }
      }

      def save(topic: Topic, pointers: Nem[Partition, Offset], timestamp: Instant) = {
        replicatedJournal
          .journal(topic)
          .use { _.save(pointers, timestamp) }
      }
    }
  }


  def empty[F[_] : Applicative]: ReplicatedJournalFlat[F] = new ReplicatedJournalFlat[F] {

    def topics = SortedSet.empty[Topic].pure[F]

    def pointers(topic: Topic) = TopicPointers.empty.pure[F]

    def append[A](
      key: Key,
      partitionOffset: PartitionOffset,
      timestamp: Instant,
      expireAfter: Option[ExpireAfter],
      events: Nel[EventRecord[A]]
    )(implicit W: EventualWrite[F, A]) = false.pure[F]

    def delete(
      key: Key,
      partitionOffset: PartitionOffset,
      timestamp: Instant,
      deleteTo: DeleteTo,
      origin: Option[Origin]
    ) = false.pure[F]

    def purge(
      key: Key,
      offset: Offset,
      timestamp: Instant
    ) = false.pure[F]

    def save(
      topic: Topic,
      pointers: Nem[Partition, Offset],
      timestamp: Instant
    ) = false.pure[F]
  }


  implicit class ReplicatedJournalFlatOps[F[_]](val self: ReplicatedJournalFlat[F]) extends AnyVal {

    def mapK[G[_]](fg: F ~> G, gf: G ~> F): ReplicatedJournalFlat[G] = new ReplicatedJournalFlat[G] {

      def topics = fg(self.topics)

      def pointers(topic: Topic) = fg(self.pointers(topic))

      def append[A](
        key: Key,
        partitionOffset: PartitionOffset,
        timestamp: Instant,
        expireAfter: Option[ExpireAfter],
        events: Nel[EventRecord[A]]
      )(implicit W: EventualWrite[G, A]) = {
        fg(self.append(key, partitionOffset, timestamp, expireAfter, events)(W.mapK(gf)))
      }

      def delete(
        key: Key,
        partitionOffset: PartitionOffset,
        timestamp: Instant,
        deleteTo: DeleteTo,
        origin: Option[Origin]
      ) = {
        fg(self.delete(key, partitionOffset, timestamp, deleteTo, origin))
      }

      def purge(
        key: Key,
        offset: Offset,
        timestamp: Instant
      ) = {
        fg(self.purge(key, offset, timestamp))
      }

      def save(topic: Topic, pointers: Nem[Partition, Offset], timestamp: Instant) = {
        fg(self.save(topic, pointers, timestamp))
      }
    }
  }
}