package akka.persistence.kafka.journal

import akka.persistence.PersistentRepr
import com.evolutiongaming.kafka.journal.{FromAttempt, FromBytes, ToBytes}
import com.evolutiongaming.serialization.SerializedMsg
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


  implicit def toBytesPersistentBinary[F[_] : FromAttempt]: ToBytes[F, PersistentBinary] = ToBytes.fromEncoder

  implicit def fromBytesPersistentBinary[F[_] : FromAttempt]: FromBytes[F, PersistentBinary] = FromBytes.fromDecoder


  def apply(msg: SerializedMsg, persistentRepr: PersistentRepr): PersistentBinary = {
    val manifest = ManifestOf(persistentRepr)
    PersistentBinary(
      manifest = manifest,
      writerUuid = persistentRepr.writerUuid,
      payload = msg)
  }
}
