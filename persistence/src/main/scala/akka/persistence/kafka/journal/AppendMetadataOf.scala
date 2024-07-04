package akka.persistence.kafka.journal

import akka.persistence.PersistentRepr
import cats.Applicative
import cats.data.{NonEmptyList => Nel}
import cats.syntax.all._
import com.evolutiongaming.kafka.journal.{Event, Key}


trait AppendMetadataOf[F[_]] {

  def apply[A](key: Key, prs: Nel[PersistentRepr], events: Nel[Event[A]]): F[AppendMetadata]
}

object AppendMetadataOf {

  def empty[F[_] : Applicative]: AppendMetadataOf[F] = const[F](AppendMetadata.empty.pure[F])


  def const[F[_]](value: F[AppendMetadata]): AppendMetadataOf[F] = new AppendMetadataOf[F] {
    override def apply[A](key: Key, prs: Nel[PersistentRepr], events: Nel[Event[A]]): F[AppendMetadata] =
      value
  }
}
