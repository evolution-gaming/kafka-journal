package org.apache.pekko.persistence.kafka.journal

import cats.syntax.all.*
import org.apache.pekko.persistence.PersistentRepr

object ManifestOf {

  def apply(persistentRepr: PersistentRepr): Option[String] = {
    val manifest = persistentRepr.manifest
    if (manifest === PersistentRepr.Undefined) None else manifest.some
  }
}
