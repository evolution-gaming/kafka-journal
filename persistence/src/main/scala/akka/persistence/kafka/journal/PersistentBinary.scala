package akka.persistence.kafka.journal

import java.lang.{Byte => ByteJ}

import akka.persistence.PersistentRepr
import com.evolutiongaming.kafka.journal.{Bytes, FromBytes, ToBytes}
import com.evolutiongaming.serialization.SerializedMsg
import scodec.bits.BitVector
import scodec.{Attempt, Codec, DecodeResult, Err, SizeBound, codecs}

final case class PersistentBinary(
  manifest: String,
  writerUuid: String,
  payload: SerializedMsg)


object PersistentBinary {

  private def codecCustom[A](codec: Codec[A], sizeCodec: Codec[Int] = codecs.int32): Codec[A] = new Codec[A] {

    def decode(bits: BitVector) = {
      for {
        result <- sizeCodec.decode(bits)
        size    = result.value
        _      <- Attempt.guard(size >= 0, Err(s"requires positive size, got $size"))
        bits    = result.remainder
        (bits1, remainder) = bits.splitAt(size * ByteJ.SIZE.toLong)
        result <- codec.decode(bits1)
      } yield {
        DecodeResult(result.value, remainder)
      }
    }

    def encode(value: A) = {
      for {
        bits     <- codec.encode(value)
        size      = bits.size.toInt / ByteJ.SIZE
        sizeBits <- sizeCodec.encode(size)
      } yield {
        sizeBits ++ bits
      }
    }

    val sizeBound = SizeBound.atLeast(ByteJ.SIZE.toLong)
  }

  private val codec = {
    val codecSerializedMsg = (codecs.int32 :: codecs.utf8_32 :: codecCustom(codecs.bytes)).as[SerializedMsg]
    (codecs.utf8_32 :: codecs.utf8_32 :: codecSerializedMsg).as[PersistentBinary]
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
