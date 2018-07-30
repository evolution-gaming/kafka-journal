package akka.persistence.kafka.journal

import com.evolutiongaming.kafka.journal.{Bytes, SeqNr}
import com.evolutiongaming.serialization.SerializerHelper._
import org.scalatest.{FunSuite, Matchers}

class PersistentEventSerializerSpec extends FunSuite with Matchers {

  test("toBinary & fromBinary") {

    val payload = "payload"

    val expected = PersistentEvent(
      seqNr = SeqNr.Min,
      persistentManifest = "persistentManifest",
      writerUuid = "writerUuid",
      identifier = 2,
      manifest = "manifest",
      payload = payload.getBytes(Utf8))

    val bytes = PersistentEventSerializer.toBinary(expected)
    val actual = PersistentEventSerializer.fromBinary(bytes)
    actual.copy(payload = Bytes.Empty.value) shouldEqual expected.copy(payload = Bytes.Empty.value)
    new String(actual.payload, Utf8) shouldEqual new String(expected.payload, Utf8)
  }
}
