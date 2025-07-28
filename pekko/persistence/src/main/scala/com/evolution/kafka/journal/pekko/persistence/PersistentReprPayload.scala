package com.evolution.kafka.journal.pekko.persistence

import org.apache.pekko.persistence.PersistentRepr
import org.apache.pekko.persistence.journal.Tagged

object PersistentReprPayload {

  def apply(persistentRepr: PersistentRepr): Tagged = {
    persistentRepr.payload match {
      case a: Tagged => a
      case a => Tagged(a, Set.empty)
    }
  }
}
