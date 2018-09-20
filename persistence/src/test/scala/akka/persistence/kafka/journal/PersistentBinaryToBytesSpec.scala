package akka.persistence.kafka.journal

import akka.persistence.kafka.journal.FixEqualityHelper._
import com.evolutiongaming.kafka.journal.FixEquality.Implicits._
import com.evolutiongaming.kafka.journal.FromBytes.Implicits._
import com.evolutiongaming.kafka.journal.ToBytes.Implicits._
import com.evolutiongaming.kafka.journal.{Bytes, BytesOf, FixEquality}
import com.evolutiongaming.serialization.SerializedMsg
import org.scalatest.{FunSuite, Matchers}

class PersistentBinaryToBytesSpec extends FunSuite with Matchers {

  private implicit val fixEquality = FixEquality.array[Byte]()

  test("toBytes & fromBytes") {

    val expected = PersistentBinary(
      manifest = "persistentManifest",
      writerUuid = "writerUuid",
      payload = SerializedMsg(
        identifier = 2,
        manifest = "manifest",
        bytes = "payload".toBytes))

    def verify(bytes: Bytes) = {
      val actual = bytes.fromBytes[PersistentBinary]
      actual.fix shouldEqual expected.fix
    }

    verify(expected.toBytes)
    verify(BytesOf(getClass, "PersistentBinary.bin"))
  }
}