package akka.persistence.kafka.journal

import akka.persistence.PersistentRepr
import cats.Applicative
import cats.data.{NonEmptyList => Nel}
import cats.implicits._
import com.evolutiongaming.kafka.journal.{Event, Headers, Key}
import play.api.libs.json.JsValue

final case class MetadataAndHeaders(metadata: Option[JsValue], headers: Headers)

object MetadataAndHeaders {
  val empty: MetadataAndHeaders = MetadataAndHeaders(none[JsValue], Headers.empty)
}


trait MetadataAndHeadersOf[F[_]] {

  def apply(key: Key, prs: Nel[PersistentRepr], events: Nel[Event]): F[MetadataAndHeaders]
}

object MetadataAndHeadersOf {

  def empty[F[_] : Applicative]: MetadataAndHeadersOf[F] = const[F](MetadataAndHeaders.empty.pure[F])


  def const[F[_]](value: F[MetadataAndHeaders]): MetadataAndHeadersOf[F] = new MetadataAndHeadersOf[F] {
    def apply(key: Key, prs: Nel[PersistentRepr], events: Nel[Event]) = value
  }
}
