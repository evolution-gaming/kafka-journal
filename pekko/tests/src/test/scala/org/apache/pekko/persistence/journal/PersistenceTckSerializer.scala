package org.apache.pekko.persistence.journal

import org.apache.pekko.persistence.journal.JournalPerfSpec.Cmd
import org.apache.pekko.serialization.SerializerWithStringManifest
import scodec.*
import scodec.bits.BitVector

import java.io.NotSerializableException

class PersistenceTckSerializer extends SerializerWithStringManifest {
  import PersistenceTckSerializer.*

  def identifier = 585506118

  def manifest(a: AnyRef): String = a match {
    case _: Cmd => cmdManifest
    case _ => illegalArgument(s"Cannot serialize message of ${ a.getClass } in ${ getClass.getName }")
  }

  def toBinary(a: AnyRef): Array[Byte] = {
    a match {
      case a: Cmd => cmdCodec.encode(a).require.toByteArray
      case _ => illegalArgument(s"Cannot serialize message of ${ a.getClass } in ${ getClass.getName }")
    }
  }

  def fromBinary(bytes: Array[Byte], manifest: String): AnyRef = {
    manifest match {
      case `cmdManifest` => cmdCodec.decode(BitVector.view(bytes)).require.value
      case _ => notSerializable(s"Cannot deserialize message for manifest $manifest in ${ getClass.getName }")
    }
  }

  private def notSerializable(msg: String): Nothing = throw new NotSerializableException(msg)

  private def illegalArgument(msg: String): Nothing = throw new IllegalArgumentException(msg)
}

object PersistenceTckSerializer {

  val cmdManifest = "A"

  implicit val cmdCodec: Codec[Cmd] = {
    val codec = codecs.utf8_32 :: codecs.int32
    codec.as[Cmd]
  }
}
