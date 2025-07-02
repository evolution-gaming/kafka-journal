package com.evolution.kafka.journal

import play.api.libs.json.JsValue
import scodec.bits.ByteVector

import scala.util.Try

/**
 * Useful extension methods for [[SerdeTesting]]
 */
trait SerdeTestingSyntax {
  implicit class StringOps(val self: String) {
    def encodeUtf8Unsafe: ByteVector = {
      ByteVector.encodeUtf8(self).fold(throw _, identity)
    }
  }

  implicit class ByteVectorOps(val self: ByteVector) {
    def decodeUtf8Unsafe: String = {
      self.decodeUtf8.fold(throw _, identity)
    }

    def decodePlayJsonUnsafe(
      implicit
      playJsonCodec: JsonCodec[Try],
    ): JsValue = {
      playJsonCodec.decode.fromBytes(self).get
    }
  }
}

object SerdeTestingSyntax extends SerdeTestingSyntax
