package akka.persistence.kafka.journal

import akka.persistence.PersistentRepr
import cats.Applicative
import cats.implicits._
import com.evolutiongaming.kafka.journal.{Event, Headers, Key}
import com.evolutiongaming.nel.Nel
import play.api.libs.json.JsValue

final case class MetadataAndHeaders(metadata: Option[JsValue], headers: Headers)

object MetadataAndHeaders {
  val Empty: MetadataAndHeaders = MetadataAndHeaders(none[JsValue], Headers.Empty)
}


trait MetadataAndHeadersOf[F[_]] {

  def apply(key: Key, prs: Nel[PersistentRepr], events: Nel[Event]): F[MetadataAndHeaders]
}

object MetadataAndHeadersOf {

  def empty[F[_] : Applicative]: MetadataAndHeadersOf[F] = const[F](MetadataAndHeaders.Empty.pure[F])


  def const[F[_]](value: F[MetadataAndHeaders]): MetadataAndHeadersOf[F] = new MetadataAndHeadersOf[F] {
    def apply(key: Key, prs: Nel[PersistentRepr], events: Nel[Event]) = value
  }
}
