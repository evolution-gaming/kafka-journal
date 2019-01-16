package com.evolutiongaming.kafka.journal.util

import akka.actor.ActorSystem
import cats.effect.{Resource, Sync}
import cats.implicits._
import com.typesafe.config.Config

object ActorSystemResource {

  def apply[F[_] : Sync : FromFuture](
    name: String,
    config: Option[Config] = None): Resource[F, ActorSystem] = {

    val actorSystem = Sync[F].delay { config.fold(ActorSystem(name)) { config => ActorSystem(name, config) } }

    for {
      actorSystem <- Resource.liftF(actorSystem)
      result      <- apply(actorSystem)
    } yield result
  }


  def apply[F[_] : Sync : FromFuture](actorSystem: ActorSystem): Resource[F, ActorSystem] = {
    val release = FromFuture[F].apply { actorSystem.terminate() }.void
    val result = (actorSystem, release).pure[F]
    Resource(result)
  }
}