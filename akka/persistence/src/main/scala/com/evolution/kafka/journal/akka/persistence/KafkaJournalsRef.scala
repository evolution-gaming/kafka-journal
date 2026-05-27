package com.evolution.kafka.journal.akka.persistence

import akka.actor.ActorSystem
import cats.effect.{IO, Sync}
import cats.syntax.all.*
import com.evolution.kafka.journal.Journals
import com.evolutiongaming.catshelper.FromFuture

/**
 * Holder published by the akka-persistence plugin to the [[ActorSystem]] so direct callers (e.g.
 * components producing `purge` / `mark` actions outside of the akka-persistence flow) can obtain
 * the same `Journals[IO]` instance the plugin uses for `write` / `replay`.
 *
 * Sharing the instance ensures actions go through the same Kafka producer and are visible to the
 * plugin's own `HeadCache` on subsequent recovery.
 */
final case class KafkaJournalsRef[F[_]](journals: Journals[F])

object KafkaJournalsRef {

  def actorSystemRef[F[_]: Sync: FromFuture](
    actorSystem: ActorSystem,
  ): F[ActorSystemRef[F, KafkaJournalsRef[IO]]] = {
    for {
      ref <- Sync[F].delay { Extension(actorSystem) }
    } yield ref.fromFuture[F]
  }

  private object Extension extends ActorSystemRef.ExtensionId[KafkaJournalsRef[IO]]
}
