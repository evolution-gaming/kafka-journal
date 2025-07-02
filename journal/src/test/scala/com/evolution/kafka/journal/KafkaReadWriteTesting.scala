package com.evolution.kafka.journal

import com.evolution.kafka.journal.SerdeTesting.EncodedDataType
import com.evolution.kafka.journal.conversions.{KafkaRead, KafkaWrite}
import org.scalatest.matchers.should.Matchers.*

import scala.util.Try

/**
 * Test mixin with utilities to test [[KafkaRead]] and [[KafkaWrite]] implementations.
 *
 * Use together with [[SerdeTesting]].
 */
trait KafkaReadWriteTesting {
  self: SerdeTesting =>

  protected implicit final val fromAttempt: FromAttempt[Try] = FromAttempt.lift[Try]
  protected implicit final val fromJsResult: FromJsResult[Try] = FromJsResult.lift[Try]

  /**
   * Same as [[verifyEncodeDecodeExample]] but for [[KafkaRead]]-[[KafkaWrite]].
   */
  protected final def verifyKafkaReadWriteExample[PAYLOAD](
    valueExample: Events[PAYLOAD],
    encodedExampleFileName: String,
    examplePayloadType: PayloadType.BinaryOrJson,
    dumpEncoded: Boolean = false,
  )(implicit
    kafkaRead: KafkaRead[Try, PAYLOAD],
    kafkaWrite: KafkaWrite[Try, PAYLOAD],
  ): Unit = {
    val encoded = kafkaWrite(valueExample).get
    encoded.payloadType shouldEqual examplePayloadType
    if (dumpEncoded) {
      dumpEncodedDataToFile(encoded.payload, encodedExampleFileName)
    }

    val bytesExample = readSerdeExampleFile(encodedExampleFileName)
    verifyEncoded(
      expected = bytesExample,
      actual = encoded.payload,
      dataType = EncodedDataType.detectByFileName(encodedExampleFileName),
    )

    val encodedExample = PayloadAndType(bytesExample, examplePayloadType)

    val decodedValueFromExample = kafkaRead(encodedExample).get
    decodedValueFromExample shouldEqual valueExample

    val decodedValueFromEncoded = kafkaRead(encoded).get
    decodedValueFromEncoded shouldEqual valueExample
    ()
  }

  /**
   * Same as [[verifyDecodeExample]] but for [[KafkaRead]].
   */
  protected final def verifyKafkaReadExample[PAYLOAD](
    valueExample: Events[PAYLOAD],
    examplePayloadType: PayloadType.BinaryOrJson,
    encodedExampleFileName: String,
  )(implicit
    kafkaRead: KafkaRead[Try, PAYLOAD],
  ): Unit = {
    val bytesExample = readSerdeExampleFile(encodedExampleFileName)
    val decodedValue = kafkaRead(PayloadAndType(bytesExample, examplePayloadType)).get
    decodedValue shouldEqual valueExample
    ()
  }
}
