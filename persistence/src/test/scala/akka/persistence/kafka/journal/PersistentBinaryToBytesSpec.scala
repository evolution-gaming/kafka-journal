package akka.persistence.kafka.journal

import cats.implicits._
import com.evolutiongaming.kafka.journal.FromBytes.implicits._
import com.evolutiongaming.kafka.journal.ToBytes.implicits._
import com.evolutiongaming.kafka.journal.ByteVectorOf
import com.evolutiongaming.serialization.SerializedMsg
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import scodec.bits.ByteVector

import scala.util.{Success, Try}

class PersistentBinaryToBytesSpec extends AnyFunSuite with Matchers {
  import PersistentBinaryToBytesSpec._

  test("toBytes & fromBytes") {

    val expected = PersistentBinary(
      manifest = Some("persistentManifest"),
      writerUuid = "writerUuid",
      payload = SerializedMsg(
        identifier = 2,
        manifest = "manifest",
        bytes = "payload".encodeStr))

    def verify(bytes: ByteVector) = {
      val actual = bytes.fromBytes[Try, PersistentBinary]
      actual shouldEqual Success(expected)
    }

    verify(expected.toBytes[Try].get)
    verify(ByteVectorOf[Try](getClass, "PersistentBinary.bin").get)
  }
}

object PersistentBinaryToBytesSpec {

  implicit class StrOps(val self: String) extends AnyVal {
    def encodeStr: ByteVector = ByteVector.encodeUtf8(self).right.get
  }
}