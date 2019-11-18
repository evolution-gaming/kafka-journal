package com.evolutiongaming.kafka.journal.eventual

import cats.Applicative
import cats.effect.Resource
import cats.implicits._
import com.evolutiongaming.skafka.Topic

trait ReplicatedJournal[F[_]] {

  def topics: F[Iterable[Topic]]

  def journal(topic: Topic): Resource[F, ReplicatedTopicJournal[F]]
}

object ReplicatedJournal {

  def apply[F[_] : Applicative](replicatedJournal: ReplicatedJournalOld[F]): ReplicatedJournal[F] = {

    new ReplicatedJournal[F] {

      def topics = replicatedJournal.topics

      def journal(topic: Topic) = {
        val replicatedTopicJournal = ReplicatedTopicJournal(topic, replicatedJournal)
        Resource.liftF(replicatedTopicJournal.pure[F])
      }
    }
  }
}