package com.evolutiongaming.kafka.journal

import akka.actor.{ActorSystem, ExtendedActorSystem, Extension, ExtensionId}
import cats.effect.Sync
import cats.implicits._
import com.evolutiongaming.scassandra.{DecodeByName, DecodeRow, EncodeByName, EncodeRow}
import play.api.libs.json._

final case class Origin(value: String) extends AnyVal {

  override def toString = value
}

object Origin {

  val empty: Origin = Origin("")


  implicit val writesOrigin: Writes[Origin] = Writes.of[String].contramap(_.value)

  implicit val readsOrigin: Reads[Origin] = Reads.of[String].map(Origin(_))


  implicit val encodeByNameOrigin: EncodeByName[Origin] = EncodeByName[String].imap((b: Origin) => b.value)

  implicit val decodeByNameOrigin: DecodeByName[Origin] = DecodeByName[String].map(value => Origin(value))


  implicit val encodeByNameOptOrigin: EncodeByName[Option[Origin]] = EncodeByName.opt[Origin]

  implicit val decodeByNameOptOrigin: DecodeByName[Option[Origin]] = DecodeByName.opt[Origin]


  implicit val encodeRowOrigin: EncodeRow[Origin] = EncodeRow("origin")

  implicit val decodeRowOrigin: DecodeRow[Origin] = DecodeRow("origin")


  implicit val encodeRowOptOrigin: EncodeRow[Option[Origin]] = EncodeRow("origin")

  implicit val decodeRowOptOrigin: DecodeRow[Option[Origin]] = DecodeRow("origin")


  def fromHostName(hostName: HostName): Origin = Origin(hostName.value)
  

  def hostName[F[_] : Sync]: F[Option[Origin]] = {
    for {
      hostName <- HostName.of[F]()
    } yield for {
      hostName <- hostName
    } yield {
      fromHostName(hostName)
    }
  }


  def akkaName(system: ActorSystem): Origin = Origin(system.name)


  def akkaHost[F[_] : Sync](system: ActorSystem): F[Option[Origin]] = {
    Sync[F].delay { AkkaHost.Ext(system).origin }
  }


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

    def apply[F[_] : Sync](system: ActorSystem): F[Option[Origin]] = {
      Sync[F].delay { Ext(system).origin }
    }
  }
}