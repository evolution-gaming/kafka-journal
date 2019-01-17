package com.evolutiongaming.kafka.journal.util

import akka.actor.ActorSystem
import cats.effect.{Resource, Sync}
import cats.implicits._
import com.typesafe.config.Config

object ActorSystemOf {

  def apply[F[_] : Sync : FromFuture](
    name: String,
    config: Option[Config] = None): Resource[F, ActorSystem] = {

    val system = Sync[F].delay { config.fold(ActorSystem(name)) { config => ActorSystem(name, config) } }

    for {
      system <- Resource.liftF(system)
      result <- apply(system)
    } yield result
  }


  def apply[F[_] : Sync : FromFuture](system: ActorSystem): Resource[F, ActorSystem] = {
    val release = FromFuture[F].apply { system.terminate() }.void
    val result = (system, release).pure[F]
    Resource(result)
  }
}