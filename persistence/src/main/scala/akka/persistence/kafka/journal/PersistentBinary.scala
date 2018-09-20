package akka.persistence.kafka.journal

import java.lang.{Integer => IntJ}
import java.nio.ByteBuffer

import akka.persistence.PersistentRepr
import com.evolutiongaming.kafka.journal.FromBytes.Implicits._
import com.evolutiongaming.kafka.journal.ToBytes.Implicits._
import com.evolutiongaming.kafka.journal.{Bytes, FromBytes, ToBytes}
import com.evolutiongaming.serialization.SerializedMsg
import com.evolutiongaming.serialization.SerializerHelper._

final case class PersistentBinary(
  manifest: String,
  writerUuid: String,
  payload: SerializedMsg)


object PersistentBinary {

  implicit val ToBytesImpl: ToBytes[PersistentBinary] = new ToBytes[PersistentBinary] {

    def apply(value: PersistentBinary): Bytes = {
      val persistentManifest = value.manifest.toBytes
      val writerUuid = value.writerUuid.toBytes
      val payload = value.payload
      val bytes = payload.bytes
      val manifest = payload.manifest.toBytes
      val buffer = ByteBuffer.allocate(
        IntJ.BYTES + persistentManifest.length +
          IntJ.BYTES + writerUuid.length +
          IntJ.BYTES +
          IntJ.BYTES + manifest.length +
          IntJ.BYTES + bytes.length)
      buffer.writeBytes(persistentManifest)
      buffer.writeBytes(writerUuid)
      buffer.putInt(payload.identifier)
      buffer.writeBytes(manifest)
      buffer.writeBytes(bytes)
      buffer.array()
    }
  }

  implicit val FromBytesImpl: FromBytes[PersistentBinary] = new FromBytes[PersistentBinary] {

    def apply(bytes: Bytes) = {
      val buffer = ByteBuffer.wrap(bytes)
      val persistentManifest = buffer.readBytes.fromBytes[String]
      val writerUuid = buffer.readBytes.fromBytes[String]
      val identifier = buffer.getInt()
      val manifest = buffer.readBytes.fromBytes[String]
      val payload = buffer.readBytes
      PersistentBinary(
        manifest = persistentManifest,
        writerUuid = writerUuid,
        payload = SerializedMsg(
          identifier = identifier,
          manifest = manifest,
          bytes = payload))
    }
  }


  def apply(msg: SerializedMsg, persistentRepr: PersistentRepr): PersistentBinary = {
    PersistentBinary(
      manifest = persistentRepr.manifest,
      writerUuid = persistentRepr.writerUuid,
      payload = msg)
  }
}
