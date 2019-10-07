package akka.persistence.kafka.journal

import akka.persistence.PersistentRepr
import cats.Applicative
import cats.data.{NonEmptyList => Nel}
import cats.implicits._
import com.evolutiongaming.kafka.journal.{Event, Headers, Key}
import play.api.libs.json.JsValue

import scala.concurrent.duration.FiniteDuration

// TODO expireAfter: rename MetadataAndHeaders to EventAux, EventInfo, etc
final case class MetadataAndHeaders(
  expireAfter: Option[FiniteDuration],
  metadata: Option[JsValue],
  headers: Headers)

object MetadataAndHeaders {
  val empty: MetadataAndHeaders = MetadataAndHeaders(none[FiniteDuration], none[JsValue], Headers.empty)
}


trait MetadataAndHeadersOf[F[_]] {

  def apply(key: Key, prs: Nel[PersistentRepr], events: Nel[Event]): F[MetadataAndHeaders]
}

object MetadataAndHeadersOf {

  def empty[F[_] : Applicative]: MetadataAndHeadersOf[F] = const[F](MetadataAndHeaders.empty.pure[F])


  def const[F[_]](value: F[MetadataAndHeaders]): MetadataAndHeadersOf[F] = {
    (_: Key, _: Nel[PersistentRepr], _: Nel[Event]) => value
  }
}
