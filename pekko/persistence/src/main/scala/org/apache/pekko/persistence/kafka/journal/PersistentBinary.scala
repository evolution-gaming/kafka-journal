package org.apache.pekko.persistence.kafka.journal

import com.evolution.serialization.SerializedMsg
import com.evolutiongaming.kafka.journal.{FromAttempt, FromBytes, ToBytes}
import org.apache.pekko.persistence.PersistentRepr
import scodec.*

final case class PersistentBinary(manifest: Option[String], writerUuid: String, payload: SerializedMsg)

object PersistentBinary {

  implicit val codecPersistentBinary: Codec[PersistentBinary] = {
    val codec = codecs.optional(codecs.bool, codecs.utf8_32) :: codecs.utf8_32 :: SerializedMsg.CodecSerializedMsg
    codec.as[PersistentBinary]
  }

  implicit def toBytesPersistentBinary[F[_]: FromAttempt]: ToBytes[F, PersistentBinary] = ToBytes.fromEncoder

  implicit def fromBytesPersistentBinary[F[_]: FromAttempt]: FromBytes[F, PersistentBinary] = FromBytes.fromDecoder

  def apply(msg: SerializedMsg, persistentRepr: PersistentRepr): PersistentBinary = {
    val manifest = ManifestOf(persistentRepr)
    PersistentBinary(manifest = manifest, writerUuid = persistentRepr.writerUuid, payload = msg)
  }
}
