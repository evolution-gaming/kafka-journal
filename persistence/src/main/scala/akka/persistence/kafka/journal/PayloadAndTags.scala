package akka.persistence.kafka.journal

import akka.persistence.journal.Tagged
import com.evolutiongaming.kafka.journal.Tags

object PayloadAndTags {

  def apply(payload: Any): (Any, Tags) = payload match {
    case Tagged(payload, tags) => (payload, tags)
    case _                     => (payload, Set.empty)
  }
}

