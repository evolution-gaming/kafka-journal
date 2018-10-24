package com.evolutiongaming.kafka.journal

import akka.actor.{ActorSystem, ExtendedActorSystem, Extension, ExtensionId}
import com.evolutiongaming.scassandra.{Decode, DecodeRow, Encode, EncodeRow}
import com.evolutiongaming.hostname
import com.evolutiongaming.kafka.journal.PlayJsonHelper._
import play.api.libs.json._

final case class Origin(value: String) extends AnyVal {
  override def toString = value
}

object Origin {

  val Empty: Origin = Origin("")

  val HostName: Option[Origin] = {
    hostname.HostName() map { hostname => Origin(hostname) }
  }


  implicit val WritesImpl: Writes[Origin] = WritesOf[String].imap(_.value)

  implicit val ReadsImpl: Reads[Origin] = ReadsOf[String].mapResult(a => JsSuccess(Origin(a)))


  implicit val EncodeImpl: Encode[Origin] = Encode[String].imap((b: Origin) => b.value)

  implicit val DecodeImpl: Decode[Origin] = Decode[String].map(value => Origin(value))


  implicit val EncodeOptImpl: Encode[Option[Origin]] = Encode.opt[Origin]

  implicit val DecodeOptImpl: Decode[Option[Origin]] = Decode.opt[Origin]


  implicit val EncodeRowImpl: EncodeRow[Origin] = EncodeRow("origin")

  implicit val DecodeRowImpl: DecodeRow[Origin] = DecodeRow("origin")


  implicit val EncodeRowOptImpl: EncodeRow[Option[Origin]] = EncodeRow("origin")

  implicit val DecodeRowOptImpl: DecodeRow[Option[Origin]] = DecodeRow("origin")


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