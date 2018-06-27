package akka.persistence.kafka.journal

import java.lang.{Integer => IntJ, Long => LongJ}
import java.nio.ByteBuffer

import com.evolutiongaming.serialization.SerializerHelper._

object PersistentEventSerializer {

  def toBinary(x: PersistentEvent): Bytes = {
    val persistentManifest = x.persistentManifest.getBytes(Utf8)
    val writerUuid = x.writerUuid.getBytes(Utf8)
    val manifest = x.manifest.getBytes(Utf8)
    val payload = x.payload
    val buffer = ByteBuffer.allocate(
      LongJ.BYTES +
        IntJ.BYTES + persistentManifest.length +
        IntJ.BYTES + writerUuid.length +
        IntJ.BYTES +
        IntJ.BYTES + manifest.length +
        IntJ.BYTES + payload.length)
    buffer.putLong(x.seqNr) // TODO
    buffer.writeBytes(persistentManifest)
    buffer.writeBytes(writerUuid)
    buffer.putInt(x.identifier)
    buffer.writeBytes(manifest)
    buffer.writeBytes(payload)
    buffer.array()
  }

  def fromBinary(bytes: Bytes): PersistentEvent = {
    val buffer = ByteBuffer.wrap(bytes)
    val seqNr = buffer.getLong()
    val persistentManifest = buffer.readString
    val writerUuid = buffer.readString
    val identifier = buffer.getInt()
    val manifest = buffer.readString
    val payload = buffer.readBytes
    PersistentEvent(
      seqNr = seqNr,
      persistentManifest = persistentManifest,
      writerUuid = writerUuid,
      identifier = identifier,
      manifest = manifest,
      payload = payload)
  }
}
