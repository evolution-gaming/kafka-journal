package akka.persistence.kafka.journal

import akka.persistence.PersistentRepr
import akka.persistence.journal.Tagged

object PersistentReprPayload {

  def apply(persistentRepr: PersistentRepr): Tagged = {
    persistentRepr.payload match {
      case a: Tagged => a
      case a => Tagged(a, Set.empty)
    }
  }
}
