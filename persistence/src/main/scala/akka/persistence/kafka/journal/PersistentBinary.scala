package akka.persistence.kafka.journal

import akka.persistence.PersistentRepr
import com.evolutiongaming.kafka.journal.{Bytes, FromBytes, ToBytes}
import com.evolutiongaming.serialization.SerializedMsg
import scodec.bits.BitVector
import scodec.codecs._

final case class PersistentBinary(
  manifest: String,
  writerUuid: String,
  payload: SerializedMsg)


object PersistentBinary {

  private val codec = {
    val codecSerializedMsg = (int32 :: utf8_32 :: variableSizeBytes(int32, bytes)).as[SerializedMsg]
    (utf8_32 :: utf8_32 :: codecSerializedMsg).as[PersistentBinary]
  }


  implicit val ToBytesImpl: ToBytes[PersistentBinary] = new ToBytes[PersistentBinary] {

    def apply(value: PersistentBinary): Bytes = {
      codec.encode(value).require.toByteArray
    }
  }

  implicit val FromBytesImpl: FromBytes[PersistentBinary] = new FromBytes[PersistentBinary] {

    def apply(bytes: Bytes) = {
      codec.decode(BitVector.view(bytes)).require.value
    }
  }


  def apply(msg: SerializedMsg, persistentRepr: PersistentRepr): PersistentBinary = {
    PersistentBinary(
      manifest = persistentRepr.manifest,
      writerUuid = persistentRepr.writerUuid,
      payload = msg)
  }
}
