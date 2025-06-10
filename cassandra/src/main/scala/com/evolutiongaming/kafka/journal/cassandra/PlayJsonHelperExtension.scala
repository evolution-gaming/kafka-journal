package com.evolutiongaming.kafka.journal.cassandra

import cats.syntax.all.*
import com.evolutiongaming.kafka.journal.JsonCodec
import com.evolutiongaming.scassandra.{DecodeByName, EncodeByName}
import play.api.libs.json.*

import scala.util.Try

private[journal] object PlayJsonHelperExtension {

  implicit def jsValueEncodeByName(
    implicit
    encode: JsonCodec.Encode[Try],
  ): EncodeByName[JsValue] =
    encodeByNameFromWrites

  implicit def jsValueDecodeByName(
    implicit
    decode: JsonCodec.Decode[Try],
  ): DecodeByName[JsValue] =
    decodeByNameFromReads

  implicit def jsValueOptEncodeByName(
    implicit
    encode: JsonCodec.Encode[Try],
  ): EncodeByName[Option[JsValue]] =
    EncodeByName.optEncodeByName

  implicit def jsValueOptDecodeByName(
    implicit
    decode: JsonCodec.Decode[Try],
  ): DecodeByName[Option[JsValue]] =
    DecodeByName.optDecodeByName

  def encodeByNameFromWrites[A](
    implicit
    writes: Writes[A],
    encode: JsonCodec.Encode[Try],
  ): EncodeByName[A] = {
    EncodeByName[String].contramap { a =>
      val jsValue = writes.writes(a)
      encode.toStr(jsValue).get
    }
  }

  def decodeByNameFromReads[A](
    implicit
    reads: Reads[A],
    decode: JsonCodec.Decode[Try],
  ): DecodeByName[A] = {
    DecodeByName[String].map { str =>
      decode
        .fromStr(str)
        .get
        .as(reads) // TODO not use `as`
    }
  }
}
