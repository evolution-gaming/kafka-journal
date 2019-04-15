package akka.persistence.kafka.journal

import com.evolutiongaming.kafka.journal.FromBytes.Implicits._
import com.evolutiongaming.kafka.journal.ToBytes.Implicits._
import com.evolutiongaming.kafka.journal.{Bytes, BytesOf}
import com.evolutiongaming.serialization.SerializedMsg
import org.scalatest.{FunSuite, Matchers}
import scodec.bits.ByteVector

class PersistentBinaryToBytesSpec extends FunSuite with Matchers {
  import PersistentBinaryToBytesSpec._

  test("toBytes & fromBytes") {

    val expected = PersistentBinary(
      manifest = Some("persistentManifest"),
      writerUuid = "writerUuid",
      payload = SerializedMsg(
        identifier = 2,
        manifest = "manifest",
        bytes = "payload".encodeStr))

    def verify(bytes: Bytes) = {
      val actual = bytes.fromBytes[PersistentBinary]
      actual shouldEqual expected
    }

    verify(expected.toBytes)
    verify(BytesOf(getClass, "PersistentBinary.bin"))
  }
}

object PersistentBinaryToBytesSpec {

  implicit class StrOps(val self: String) extends AnyVal {
    def encodeStr: ByteVector = ByteVector.encodeUtf8(self).right.get
  }
}