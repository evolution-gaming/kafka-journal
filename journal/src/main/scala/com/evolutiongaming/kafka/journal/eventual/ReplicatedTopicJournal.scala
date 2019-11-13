package com.evolutiongaming.kafka.journal.eventual

import java.time.Instant

import cats.Applicative
import cats.data.{NonEmptyMap => Nem}
import cats.effect.Resource
import cats.implicits._
import com.evolutiongaming.kafka.journal.Key
import com.evolutiongaming.skafka.{Offset, Partition, Topic}

trait ReplicatedTopicJournal[F[_]] {

  def pointers: F[TopicPointers]

  def journal(id: String): Resource[F, ReplicatedKeyJournal[F]]

  def save(pointers: Nem[Partition, Offset], timestamp: Instant): F[Unit]
}

object ReplicatedTopicJournal {

  def apply[F[_] : Applicative](
    topic: Topic,
    replicatedJournal: ReplicatedJournal[F]
  ): ReplicatedTopicJournal[F] = {

    new ReplicatedTopicJournal[F] {

      def pointers = replicatedJournal.pointers(topic)

      def journal(id: String) = {
        val key = Key(id = id, topic = topic)
        val replicatedKeyJournal = ReplicatedKeyJournal(key, replicatedJournal)
        Resource.liftF(replicatedKeyJournal.pure[F])
      }

      def save(pointers: Nem[Partition, Offset], timestamp: Instant) = {
        replicatedJournal.save(topic, pointers, timestamp)
      }
    }
  }
}