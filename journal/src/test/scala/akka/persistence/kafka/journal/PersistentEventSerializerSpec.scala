package akka.persistence.kafka.journal

import com.evolutiongaming.serialization.SerializerHelper._
import org.scalatest.{FunSuite, Matchers}

class PersistentEventSerializerSpec extends FunSuite with Matchers {

  test("toBinary & fromBinary") {

    val payload = "payload"

    val expected = PersistentEvent(
      seqNr = 1L,
      persistentManifest = "persistentManifest",
      writerUuid = "writerUuid",
      identifier = 2,
      manifest = "manifest",
      payload = payload.getBytes(Utf8))

    val bytes = PersistentEventSerializer.toBinary(expected)
    val actual = PersistentEventSerializer.fromBinary(bytes)
    val payloadEmpty = Array.empty[Byte]
    actual.copy(payload = payloadEmpty) shouldEqual expected.copy(payload = payloadEmpty)
    new String(actual.payload, Utf8) shouldEqual new String(expected.payload, Utf8)
  }
}
