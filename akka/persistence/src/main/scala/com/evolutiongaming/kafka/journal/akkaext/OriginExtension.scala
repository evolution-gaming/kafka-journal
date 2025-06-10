package com.evolutiongaming.kafka.journal.akkaext

import akka.actor.{ActorSystem, ExtendedActorSystem, Extension, ExtensionId}
import cats.effect.Sync
import com.evolutiongaming.kafka.journal.Origin

object OriginExtension {

  def akkaName(system: ActorSystem): Origin = Origin(system.name)

  def akkaHost[F[_]: Sync](system: ActorSystem): F[Option[Origin]] =
    Sync[F].delay { AkkaHost.Ext(system).origin }

  private object AkkaHost {

    case class Ext(origin: Option[Origin]) extends Extension

    object Ext extends ExtensionId[Ext] {

      def createExtension(system: ExtendedActorSystem): Ext = {
        val address = system.provider.getDefaultAddress
        val origin = for {
          host <- address.host
          port <- address.port
        } yield Origin(s"$host:$port")
        Ext(origin)
      }
    }

    def apply[F[_]: Sync](system: ActorSystem): F[Option[Origin]] = {
      Sync[F].delay { Ext(system).origin }
    }
  }

}
