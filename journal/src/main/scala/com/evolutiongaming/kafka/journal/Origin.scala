package com.evolutiongaming.kafka.journal

import akka.actor.{ActorSystem, ExtendedActorSystem, Extension, ExtensionId}
import com.evolutiongaming.scassandra.{DecodeByName, DecodeRow, EncodeByName, EncodeRow}
import com.evolutiongaming.kafka.journal.PlayJsonHelper._
import play.api.libs.json._

final case class Origin(value: String) extends AnyVal {

  override def toString = value
}

object Origin {

  val Empty: Origin = Origin("")

  val HostName: Option[Origin] = {
    com.evolutiongaming.kafka.journal.HostName() map { hostname => Origin(hostname.value) }
  }


  implicit val WritesOrigin: Writes[Origin] = WritesOf[String].contramap(_.value)

  implicit val ReadsOrigin: Reads[Origin] = ReadsOf[String].map(Origin(_))


  implicit val EncodeByNameOrigin: EncodeByName[Origin] = EncodeByName[String].imap((b: Origin) => b.value)

  implicit val DecodeByNameOrigin: DecodeByName[Origin] = DecodeByName[String].map(value => Origin(value))


  implicit val EncodeByNameOptOrigin: EncodeByName[Option[Origin]] = EncodeByName.opt[Origin]

  implicit val DecodeByNameOptOrigin: DecodeByName[Option[Origin]] = DecodeByName.opt[Origin]


  implicit val EncodeRowOrigin: EncodeRow[Origin] = EncodeRow("origin")

  implicit val DecodeRowOrigin: DecodeRow[Origin] = DecodeRow("origin")


  implicit val EncodeRowOptOrigin: EncodeRow[Option[Origin]] = EncodeRow("origin")

  implicit val DecodeRowOptOrigin: DecodeRow[Option[Origin]] = DecodeRow("origin")


  object AkkaHost {

    private case class Ext(origin: Option[Origin]) extends Extension

    private object Ext extends ExtensionId[Ext] {

      def createExtension(system: ExtendedActorSystem): Ext = {
        val address = system.provider.getDefaultAddress
        val origin = for {
          host <- address.host
          port <- address.port
        } yield Origin(s"$host:$port")
        Ext(origin)
      }
    }

    def apply(system: ActorSystem): Option[Origin] = {
      Ext(system).origin
    }
  }

  object AkkaName {
    def apply(system: ActorSystem): Origin = Origin(system.name)
  }
}