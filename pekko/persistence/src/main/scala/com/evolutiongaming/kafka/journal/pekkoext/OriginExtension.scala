package com.evolutiongaming.kafka.journal.pekkoext

import org.apache.pekko.actor.{ActorSystem, ExtendedActorSystem, Extension, ExtensionId}
import cats.effect.Sync
import com.evolutiongaming.kafka.journal.Origin

object OriginExtension {

  def pekkoName(system: ActorSystem): Origin = Origin(system.name)

  def pekkoHost[F[_]: Sync](system: ActorSystem): F[Option[Origin]] =
    Sync[F].delay { PekkoHost.Ext(system).origin }

  private object PekkoHost {

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
