package org.apache.pekko.persistence.kafka.journal

import cats.Applicative
import cats.data.NonEmptyList as Nel
import cats.syntax.all.*
import com.evolution.kafka.journal.{Event, Key}
import org.apache.pekko.persistence.PersistentRepr

trait AppendMetadataOf[F[_]] {

  def apply[A](key: Key, prs: Nel[PersistentRepr], events: Nel[Event[A]]): F[AppendMetadata]
}

object AppendMetadataOf {

  def empty[F[_]: Applicative]: AppendMetadataOf[F] = const[F](AppendMetadata.empty.pure[F])

  def const[F[_]](value: F[AppendMetadata]): AppendMetadataOf[F] = new AppendMetadataOf[F] {
    override def apply[A](key: Key, prs: Nel[PersistentRepr], events: Nel[Event[A]]): F[AppendMetadata] =
      value
  }
}
