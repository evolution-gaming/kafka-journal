package akka.persistence.kafka.journal

import akka.persistence.PersistentRepr
import com.evolutiongaming.kafka.journal.{Bytes, FromBytes, ToBytes}
import com.evolutiongaming.serialization.SerializedMsg
import scodec.bits.BitVector
import scodec.{Codec, codecs}

final case class PersistentBinary(
  manifest: Option[String],
  writerUuid: String,
  payload: SerializedMsg)


object PersistentBinary {

  implicit val CodecPersistentBinary: Codec[PersistentBinary] = {
    val codec = codecs.optional(codecs.bool, codecs.utf8_32) :: codecs.utf8_32 :: Codec[SerializedMsg]
    codec.as[PersistentBinary]
  }


  implicit val ToBytesPersistentBinary: ToBytes[PersistentBinary] = new ToBytes[PersistentBinary] {

    def apply(value: PersistentBinary): Bytes = {
      CodecPersistentBinary.encode(value).require.toByteArray
    }
  }

  implicit val FromBytesPersistentBinary: FromBytes[PersistentBinary] = new FromBytes[PersistentBinary] {

    def apply(bytes: Bytes) = {
      CodecPersistentBinary.decode(BitVector.view(bytes)).require.value
    }
  }


  def apply(msg: SerializedMsg, persistentRepr: PersistentRepr): PersistentBinary = {
    val manifest = ManifestOf(persistentRepr)
    PersistentBinary(
      manifest = manifest,
      writerUuid = persistentRepr.writerUuid,
      payload = msg)
  }
}
