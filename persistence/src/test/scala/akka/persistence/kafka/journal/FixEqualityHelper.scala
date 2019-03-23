package akka.persistence.kafka.journal

import com.evolutiongaming.kafka.journal.FixEquality
import com.evolutiongaming.kafka.journal.FixEquality.Implicits._
import com.evolutiongaming.serialization.SerializedMsg

object FixEqualityHelper {

  implicit def PersistentEventFixEquality(implicit fixEquality: FixEquality[SerializedMsg]): FixEquality[PersistentBinary] = {
    new FixEquality[PersistentBinary] {
      def apply(a: PersistentBinary) = a.copy(payload = a.payload.fix)
    }
  }
}
