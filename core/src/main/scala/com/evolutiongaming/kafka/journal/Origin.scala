package com.evolutiongaming.kafka.journal

import cats.effect.Sync
import cats.syntax.all.*
import play.api.libs.json.*

/**
 * Name of the host, which produced an event or a snapshot.
 *
 * There is no formal requirement of which name is to be used, so it could be `/etc/hostname`, IP
 * address or even the underlying actor system name.
 */
final case class Origin(value: String) extends AnyVal {

  override def toString: String = value
}

object Origin {

  val empty: Origin = Origin("")

  implicit val writesOrigin: Writes[Origin] = Writes.of[String].contramap(_.value)

  implicit val readsOrigin: Reads[Origin] = Reads.of[String].map(Origin(_))

  def fromHostName(hostName: HostName): Origin = Origin(hostName.value)

  def hostName[F[_]: Sync]: F[Option[Origin]] = {
    for {
      hostName <- HostName.of[F]()
    } yield for {
      hostName <- hostName
    } yield {
      fromHostName(hostName)
    }
  }
}
