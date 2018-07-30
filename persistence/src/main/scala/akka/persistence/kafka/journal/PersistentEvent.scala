package akka.persistence.kafka.journal

import akka.persistence.PersistentRepr
import com.evolutiongaming.kafka.journal.SeqNr
import com.evolutiongaming.serialization.SerializedMsg

final case class PersistentEvent(
  seqNr: SeqNr, // TODO really ?
  persistentManifest: String,
  writerUuid: String,
  identifier: Int,
  manifest: String,
  payload: Array[Byte])


object PersistentEvent {

  def apply(msg: SerializedMsg, persistentRepr: PersistentRepr): PersistentEvent = {
    PersistentEvent(
      seqNr = SeqNr(persistentRepr.sequenceNr),
      persistentManifest = persistentRepr.manifest,
      writerUuid = persistentRepr.writerUuid,
      identifier = msg.identifier,
      manifest = msg.manifest,
      payload = msg.bytes)
  }
}