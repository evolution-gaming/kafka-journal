package com.evolution.kafka.journal

import com.evolution.kafka.journal.FromBytes.implicits.*
import com.evolution.kafka.journal.ToBytes.implicits.*
import org.scalatest.matchers.should.Matchers.*
import scodec.bits.ByteVector

import java.nio.file.{Files, Paths}
import scala.util.Try

/**
 * Test mixin with utilities to test serialization formats
 */
trait SerdeTesting extends SerdeTestingSyntax {
  import SerdeTesting.EncodedDataType

  /**
   * Class to be used as a context for retrieving serialization example files from the class-path.
   *
   * The files have to reside in the same package as the class.
   *
   * By default, the concrete test class extending the trait is used.
   *
   * It could be overridden to reuse test files from another package or module.
   */
  protected def serdeExampleFileContext: Class[?] = this.getClass

  /**
   * `JsonCodec[Try]` instance to be used for serializing Play-Json types
   */
  protected implicit def playJsonCodec: JsonCodec[Try] = JsonCodec.default

  /**
   * Verifies [[ToBytes]]-[[FromBytes]] encoding-decoding of a value example against an encoded file
   * example from the class-path.
   *
   * File type is detected using [[EncodedDataType]]. For binary and text files, exact data match is
   * required. For JSON files, structural comparison is performed.
   *
   * It should be used to verify the current format version for the type. For verifying decoding of
   * legacy formats, use [[verifyDecodeExample]].
   *
   * @param valueExample
   *   value example to be encoded-decoded
   * @param encodedExampleFileName
   *   file name for the file with an encoded data example
   * @param dumpEncoded
   *   If true, actual encoded data is written to a file with the same name in the current process
   *   directory (most likely - repository root). This is useful for debugging format discrepancies,
   *   especially with binary data. Or it could be used to create initial test file examples.
   * @tparam T
   *   encoded-decoded value type
   *
   * @see
   *   [[serdeExampleFileContext]]
   */
  protected final def verifyEncodeDecodeExample[T: ToBytes[Try, *]: FromBytes[Try, *]](
    valueExample: T,
    encodedExampleFileName: String,
    dumpEncoded: Boolean = false,
  ): Unit = {
    val encodedBytes = valueExample.toBytes[Try].get
    if (dumpEncoded) {
      dumpEncodedDataToFile(encodedBytes, encodedExampleFileName)
    }

    val bytesExample = readSerdeExampleFile(encodedExampleFileName)
    verifyEncoded(
      expected = bytesExample,
      actual = encodedBytes,
      dataType = EncodedDataType.detectByFileName(encodedExampleFileName),
    )

    val decodedValueFromExample = bytesExample.fromBytes[Try, T].get
    decodedValueFromExample shouldEqual valueExample

    val decodedValueFromEncoded = encodedBytes.fromBytes[Try, T].get
    decodedValueFromEncoded shouldEqual valueExample
    ()
  }

  /**
   * Verify encoded binary data against an example.
   *
   * For binary and text data, exact data match is required. For JSON data, structural comparison is
   * performed.
   *
   * @param expected
   *   expected binary data
   * @param actual
   *   actual binary data
   * @param dataType
   *   determines the data comparison logic
   */
  protected final def verifyEncoded(expected: ByteVector, actual: ByteVector, dataType: EncodedDataType): Unit = {
    dataType match {
      case EncodedDataType.Binary =>
        actual shouldEqual expected
      case EncodedDataType.Text =>
        actual.decodeUtf8Unsafe shouldEqual expected.decodeUtf8Unsafe
      case EncodedDataType.Json =>
        actual.decodePlayJsonUnsafe shouldEqual expected.decodePlayJsonUnsafe
    }
    ()
  }

  /**
   * Verifies [[FromBytes]] decoding of a value example against an encoded file example from the
   * class-path.
   *
   * It could be used to verify decoding of legacy formats. For current formats use
   * [[verifyEncodeDecodeExample]].
   *
   * @param valueExample
   *   value example to be compared against the decoded data
   * @param encodedExampleFileName
   *   file name for the file with an encoded data example
   * @tparam T
   *   decoded value type
   *
   * @see
   *   [[serdeExampleFileContext]]
   */
  protected final def verifyDecodeExample[T: FromBytes[Try, *]](
    valueExample: T,
    encodedExampleFileName: String,
  ): Unit = {
    val bytesExample = readSerdeExampleFile(encodedExampleFileName)
    val decodedValue = bytesExample.fromBytes[Try, T].get
    decodedValue shouldEqual valueExample
    ()
  }

  /**
   * Reads an encoded data example file from the class-path.
   *
   * @param fileName
   *   file name
   * @see
   *   [[serdeExampleFileContext]]
   */
  protected final def readSerdeExampleFile(fileName: String): ByteVector = {
    val is = Option(serdeExampleFileContext.getResourceAsStream(fileName))
      .getOrElse(sys.error(s"SerDe example file not found: $fileName"))
    try {
      ByteVector.view(is.readAllBytes())
    } finally {
      is.close()
    }
  }

  /**
   * Reads an encoded data example file from the class-path as a [[Payload]] instance.
   *
   * @param fileName
   *   file name
   * @param tpe
   *   expected payload type
   */
  protected final def readSerdeExampleFileAsPayload(fileName: String, tpe: PayloadType): Payload = {
    val bytes = readSerdeExampleFile(fileName)
    tpe match {
      case PayloadType.Binary => Payload.binary(bytes)
      case PayloadType.Json => Payload.Json(bytes.decodePlayJsonUnsafe)
      case PayloadType.Text => Payload.text(bytes.decodeUtf8Unsafe)
    }
  }

  /**
   * Writes encoded binary data to a file in the current process directory (most likely - repository
   * root).
   *
   * This is useful for debugging format discrepancies, especially with binary data. Or it could be
   * used to create initial test file examples.
   *
   * @param bytes
   *   binary data to write
   * @param fileName
   *   file name
   */
  protected final def dumpEncodedDataToFile(bytes: ByteVector, fileName: String): Unit = {
    Files.write(Paths.get(fileName), bytes.toArray)
    ()
  }
}

object SerdeTesting {

  // separate from PayloadType to differentiate main model and testing concerns
  /**
   * Type of an encoded data example.
   *
   * Supported:
   *   - binary - `.bin` files
   *   - text - `.txt` files
   *   - JSON - `.json` files
   */
  sealed trait EncodedDataType {
    def fileExtension: String
  }
  object EncodedDataType {
    case object Binary extends EncodedDataType {
      override def fileExtension: Tag = "bin"
    }
    case object Json extends EncodedDataType {
      override def fileExtension: Tag = "json"
    }
    case object Text extends EncodedDataType {
      override def fileExtension: Tag = "txt"
    }

    val values: Vector[EncodedDataType] = Vector(Binary, Json, Text)

    def detectByFileName(name: String): EncodedDataType = {
      values.find(tpe => name.endsWith("." + tpe.fileExtension)).getOrElse(
        sys.error(s"unable to detect encoded data type by file extensions, " +
          s"supported: ${ values.map(_.fileExtension).mkString(", ") }"),
      )
    }

    def fromPayloadType(payloadType: PayloadType): EncodedDataType = payloadType match {
      case PayloadType.Binary => EncodedDataType.Binary
      case PayloadType.Text => EncodedDataType.Text
      case PayloadType.Json => EncodedDataType.Json
    }
  }
}
