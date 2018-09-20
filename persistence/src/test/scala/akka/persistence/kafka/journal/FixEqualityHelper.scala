package akka.persistence.kafka.journal

import com.evolutiongaming.kafka.journal.FixEquality.Implicits._
import com.evolutiongaming.kafka.journal.{Bytes, FixEquality}
import com.evolutiongaming.serialization.SerializedMsg

object FixEqualityHelper {

  implicit def serializedMsgFixEquality(implicit fixEquality: FixEquality[Bytes]): FixEquality[SerializedMsg] = {
    new FixEquality[SerializedMsg] {
      def apply(a: SerializedMsg) = a.copy(bytes = a.bytes.fix)
    }
  }

  implicit def PersistentEventFixEquality(implicit fixEquality: FixEquality[SerializedMsg]): FixEquality[PersistentBinary] = {
    new FixEquality[PersistentBinary] {
      def apply(a: PersistentBinary) = a.copy(payload = a.payload.fix)
    }
  }
}
