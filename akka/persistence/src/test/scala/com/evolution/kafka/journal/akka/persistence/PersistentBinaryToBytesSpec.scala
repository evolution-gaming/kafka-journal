package com.evolution.kafka.journal.akka.persistence

import cats.syntax.all.*
import com.evolution.kafka.journal.SerdeTesting
import com.evolutiongaming.serialization.SerializedMsg
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class PersistentBinaryToBytesSpec extends AnyFunSuite with Matchers with SerdeTesting {

  test("toBytes & fromBytes") {

    val expected = PersistentBinary(
      manifest = "persistentManifest".some,
      writerUuid = "writerUuid",
      payload = SerializedMsg(identifier = 2, manifest = "manifest", bytes = "payload".encodeUtf8Unsafe),
    )

    verifyEncodeDecodeExample(
      valueExample = expected,
      encodedExampleFileName = "PersistentBinary.bin",
//      dumpEncoded = true,
    )
  }
}
