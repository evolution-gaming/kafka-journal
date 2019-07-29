package akka.persistence.kafka.journal

import akka.persistence.PersistentRepr

object ManifestOf {

  def apply(persistentRepr: PersistentRepr): Option[String] = {
    val manifest = persistentRepr.manifest
    if (manifest == PersistentRepr.Undefined) None else Some(manifest)
  }
}
