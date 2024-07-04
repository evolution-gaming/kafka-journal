package akka.persistence.kafka.journal

import akka.persistence.PersistentRepr
import cats.syntax.all.*

object ManifestOf {

  def apply(persistentRepr: PersistentRepr): Option[String] = {
    val manifest = persistentRepr.manifest
    if (manifest === PersistentRepr.Undefined) None else manifest.some
  }
}
