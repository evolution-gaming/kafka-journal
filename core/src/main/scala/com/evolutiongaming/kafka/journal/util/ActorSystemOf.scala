package com.evolutiongaming.kafka.journal.util

import akka.actor.ActorSystem
import cats.effect.syntax.all.*
import cats.effect.{Resource, Sync}
import cats.syntax.all.*
import com.evolutiongaming.catshelper.FromFuture
import com.typesafe.config.Config

private[journal] object ActorSystemOf {

  def apply[F[_]: Sync: FromFuture](
    name: String,
    config: Option[Config] = None,
  ): Resource[F, ActorSystem] = {

    val system = Sync[F].delay { config.fold(ActorSystem(name)) { config => ActorSystem(name, config) } }

    for {
      system <- system.toResource
      result <- apply(system)
    } yield result
  }

  def apply[F[_]: Sync: FromFuture](system: ActorSystem): Resource[F, ActorSystem] = {
    val release = FromFuture[F].apply { system.terminate() }.void
    val result  = (system, release).pure[F]
    Resource(result)
  }
}
