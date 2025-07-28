package com.evolution.kafka.journal.pekko.persistence

import cats.syntax.all.*
import org.apache.pekko.persistence.PersistentRepr

object ManifestOf {

  def apply(persistentRepr: PersistentRepr): Option[String] = {
    val manifest = persistentRepr.manifest
    if (manifest === PersistentRepr.Undefined) None else manifest.some
  }
}
