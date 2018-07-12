package akka.persistence.kafka.journal

import com.evolutiongaming.kafka.journal.Alias.Bytes
import com.evolutiongaming.serialization.SerializerHelper._
import org.scalatest.{FunSuite, Matchers}

class PersistentEventSerializerSpec extends FunSuite with Matchers {

  test("toBinary & fromBinary") {

    val payload = "payload"

    val expected = PersistentEvent(
      seqNr = 1l,
      persistentManifest = "persistentManifest",
      writerUuid = "writerUuid",
      identifier = 2,
      manifest = "manifest",
      payload = payload.getBytes(Utf8))

    val bytes = PersistentEventSerializer.toBinary(expected)
    val actual = PersistentEventSerializer.fromBinary(bytes)
    actual.copy(payload = Bytes.Empty) shouldEqual expected.copy(payload = Bytes.Empty)
    new String(actual.payload, Utf8) shouldEqual new String(expected.payload, Utf8)
  }
}
